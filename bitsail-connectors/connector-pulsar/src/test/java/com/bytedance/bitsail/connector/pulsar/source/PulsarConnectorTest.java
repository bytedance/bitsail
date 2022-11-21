package com.bytedance.bitsail.connector.pulsar.source;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.option.ReaderOptions;
import com.bytedance.bitsail.common.typeinfo.TypeInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfoUtils;
import com.bytedance.bitsail.common.util.JsonSerializer;
import com.bytedance.bitsail.connector.pulsar.source.enumerator.cursor.StartCursor;
import com.bytedance.bitsail.connector.pulsar.source.reader.deserializer.PulsarDeserializationSchema;
import com.bytedance.bitsail.connector.pulsar.testutils.IntegerSource;
import com.bytedance.bitsail.test.connector.test.EmbeddedFlinkCluster;
import com.bytedance.bitsail.test.connector.test.utils.JobConfUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.pulsar.FlinkPulsarSink;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarSerializationSchemaWrapper;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PulsarContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.bytedance.bitsail.connector.pulsar.common.config.v1.PulsarOptionsV1.PULSAR_ADMIN_URL;
import static com.bytedance.bitsail.connector.pulsar.common.config.v1.PulsarOptionsV1.PULSAR_SERVICE_URL;
import static java.time.temporal.ChronoUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.testcontainers.containers.PulsarContainer.BROKER_HTTP_PORT;

public class PulsarConnectorTest extends EmbeddedFlinkCluster {
  private static final Logger log = LoggerFactory.getLogger(PulsarConnectorTest.class);

  private static final String PULSAR_SOURCE_VERSION = "4.9.4";
  private static final DockerImageName PULSAR_DOCKER_IMAGE = DockerImageName
      .parse("apache/rocketmq:4.9.4")
      .withTag(PULSAR_SOURCE_VERSION);
  private static final Network NETWORK = Network.newNetwork();

  private static final String DEFAULT_PRODUCE_GROUP = "bitsail";
  private static final String DEFAULT_TOPIC = "TBW102";
  private static final int DEFAULT_QUEUE_NUMBER = 5;
  private PulsarContainer pulsarService;
  private static String serviceUrl;
  private static String adminUrl;

  private PulsarAdmin admin;

  @Before
  public void before() throws Throwable {
    log.info("Starting PulsarTestBase ");
    final String pulsarImage =
	System.getProperty("pulsar.systemtest.image", "apachepulsar/pulsar:2.8.0");
    DockerImageName pulsar =
	DockerImageName.parse(pulsarImage).asCompatibleSubstituteFor("apachepulsar/pulsar");
    pulsarService = new PulsarContainer(pulsar);
    pulsarService.withClasspathResourceMapping(
	"pulsar/txnStandalone.conf", "/pulsar/conf/standalone.conf", BindMode.READ_ONLY);
    pulsarService.waitingFor(
	new HttpWaitStrategy()
	    .forPort(BROKER_HTTP_PORT)
	    .forStatusCode(200)
	    .forPath("/admin/v2/namespaces/public/default")
	    .withStartupTimeout(Duration.of(40, SECONDS)));
    pulsarService.start();
    pulsarService.followOutput(new Slf4jLogConsumer(log));
    serviceUrl = pulsarService.getPulsarBrokerUrl();
    adminUrl = pulsarService.getHttpServiceUrl();

    log.info(
	"Successfully started pulsar service at cluster "
	    + pulsarService.getContainerName());
  }


  @Test
  public void testPulsarSourceV1() throws Exception {
    BitSailConfiguration jobConf = JobConfUtils.fromClasspath("bitsail_pulsar_print.json");
    jobConf.set(PULSAR_SERVICE_URL, serviceUrl);
    jobConf.set(PULSAR_ADMIN_URL, adminUrl);
    PulsarSourceV1 pulsarSource = new PulsarSourceV1();
    TypeInfo<?>[] typeInfos = TypeInfoUtils.getTypeInfos(pulsarSource.createTypeInfoConverter(),
	jobConf.get(ReaderOptions.BaseReaderOptions.COLUMNS));

    startProduceMessages(typeInfos);
    EmbeddedFlinkCluster.submitJob(jobConf);
  }



  private void startProduceMessages(TypeInfo<?>[] typeInfos) throws PulsarClientException {
    PulsarClient client =
	PulsarClient.builder().enableTransaction(true).serviceUrl(serviceUrl).build();
    Producer<byte[]> producer = client.newProducer(Schema.BYTES).topic(DEFAULT_TOPIC).create();
    ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
    scheduledExecutor.scheduleAtFixedRate(
	new Thread(() -> {
	  try {
	    for (int i = 0; i < 100; i++) {
	      producer.send(fakeJsonObject(i, typeInfos));
	    }
	  } catch (Exception e) {
	    log.error("Produce failed.", e);
	  }
	}), 0, 15, TimeUnit.SECONDS);
  }

  private static byte[] fakeJsonObject(int index, TypeInfo<?>[] typeInfos) {
    Map<Object, Object> demo = Maps.newHashMap();
    demo.put("id", index);
    return JsonSerializer.serialize(demo).getBytes();
  }



  @Test
  public void testPulsarSource() throws Exception {
    final String topic = "ExactlyOnceTopicSource" + UUID.randomUUID();

    // produce messages
    PulsarClient client =
	PulsarClient.builder().enableTransaction(true).serviceUrl(serviceUrl).build();
    Producer<String> producer = client.newProducer(Schema.STRING).topic(topic).create();
    for (int i = 0; i < 100; i++) {
      producer.send(String.valueOf(i));
    }

    PulsarSource<String> source = PulsarSource.builder()
	.setServiceUrl(serviceUrl)
	.setAdminUrl(adminUrl)
	.setStartCursor(StartCursor.earliest())
	.setTopics(topic)
	.setDeserializationSchema(PulsarDeserializationSchema.flinkSchema(new SimpleStringSchema()))
	.setSubscriptionName("my-subscription")
	.setSubscriptionType(SubscriptionType.Exclusive)
	.build();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    DataStreamSource<String> ds = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Pulsar Source");
    ds.print();
    env.execute("Exactly once test");

  }

  @Test
  public void testPulsarSink() throws Exception {
    testSink(1);
  }

  protected void testSink(int sinksCount) throws Exception {
    final String topic = "ExactlyOnceTopicSink" + UUID.randomUUID();
    final int numElements = 1000;

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.enableCheckpointing(500);
    env.setParallelism(1);
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0));

    // process exactly failAfterElements number of elements and then shutdown Pulsar broker and
    // fail application
    List<Integer> expectedElements = getIntegersSequence(numElements);

    DataStream<Integer> inputStream =
	env.addSource(new IntegerSource(numElements));

    for (int i = 0; i < sinksCount; i++) {
      ClientConfigurationData clientConfigurationData = new ClientConfigurationData();
      clientConfigurationData.setServiceUrl(serviceUrl);
      clientConfigurationData.setEnableTransaction(true);
      SinkFunction<Integer> sink =
	  new FlinkPulsarSink<>(
	      adminUrl,
	      Optional.of(topic),
	      clientConfigurationData,
	      new Properties(),
	      new PulsarSerializationSchemaWrapper.Builder<>(
		  (SerializationSchema<Integer>)
		      element -> Schema.INT32.encode(element))
		  .useAtomicMode(DataTypes.INT())
		  .build());
      inputStream.addSink(sink);
    }

    env.execute("Exactly once test");
    for (int i = 0; i < sinksCount; i++) {
      // assert that before failure we successfully snapshot/flushed all expected elements
      assertExactlyOnceForTopic(topic, expectedElements, 60000L);
    }
  }

  private List<Integer> getIntegersSequence(int size) {
    List<Integer> result = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      result.add(i);
    }
    return result;
  }

  /**
   * We manually handle the timeout instead of using JUnit's timeout to return failure instead of
   * timeout error. After timeout we assume that there are missing records and there is a bug, not
   * that the test has run out of time.
   */
  public void assertExactlyOnceForTopic(
      String topic, List<Integer> expectedElements, long timeoutMillis) throws Exception {

    long startMillis = System.currentTimeMillis();
    List<Integer> actualElements = new ArrayList<>();

    // until we timeout...
    PulsarClient client =
	PulsarClient.builder().enableTransaction(true).serviceUrl(serviceUrl).build();
    Consumer<Integer> test =
	client.newConsumer(Schema.INT32)
	    .topic(topic)
	    .subscriptionName("test-exactly" + UUID.randomUUID())
	    .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
	    .subscribe();
    while (System.currentTimeMillis() < startMillis + timeoutMillis) {
      // query pulsar for new records ...
      Message<Integer> message = test.receive();
      log.info(
	  "consume the message {} with the value {}",
	  message.getMessageId(),
	  message.getValue());
      actualElements.add(message.getValue());
      // succeed if we got all expectedElements
      if (actualElements.size() == expectedElements.size()) {
	assertEquals(expectedElements, actualElements);
	return;
      }
      if (actualElements.equals(expectedElements)) {
	return;
      }
      // fail early if we already have too many elements
      if (actualElements.size() > expectedElements.size()) {
	break;
      }
    }

    fail(
	String.format(
	    "Expected %s, but was: %s",
	    formatElements(expectedElements), formatElements(actualElements)));
  }

  private String formatElements(List<Integer> elements) {
    if (elements.size() > 50) {
      return String.format("number of elements: <%s>", elements.size());
    } else {
      return String.format("elements: <%s>", elements);
    }
  }


  @After
  public void after() {
    log.info("-------------------------------------------------------------------------");
    log.info("    Shut down PulsarTestBase ");
    log.info("-------------------------------------------------------------------------");

    TestStreamEnvironment.unsetAsContext();

    if (pulsarService != null) {
      pulsarService.stop();
    }

    log.info("-------------------------------------------------------------------------");
    log.info("    PulsarTestBase finished");
    log.info("-------------------------------------------------------------------------");
  }
}