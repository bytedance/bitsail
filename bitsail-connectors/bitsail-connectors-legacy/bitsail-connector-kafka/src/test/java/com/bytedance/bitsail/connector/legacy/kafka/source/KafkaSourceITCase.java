/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bytedance.bitsail.connector.legacy.kafka.source;

import com.bytedance.bitsail.batch.file.parser.PbBytesParser;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.parser.option.RowParserOptions;
import com.bytedance.bitsail.test.connector.test.EmbeddedFlinkCluster;
import com.bytedance.bitsail.test.connector.test.testcontainers.kafka.KafkaCluster;
import com.bytedance.bitsail.test.connector.test.utils.JobConfUtils;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import java.io.File;
import java.net.URI;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created 2022/8/31
 */
public class KafkaSourceITCase {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaSourceITCase.class);
  private static final int TOTAL_SEND_COUNT = 300;
  private final String topicName = "testTopic";
  private final KafkaCluster kafkaCluster = new KafkaCluster();
  private ScheduledThreadPoolExecutor produceService;

  private static String constructARecord(int index) {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put("ID", index);
    jsonObject.put("NAME", "text_" + index);
    jsonObject.put("DATE", System.currentTimeMillis());
    return jsonObject.toJSONString();
  }

  @Before
  public void before() {
    kafkaCluster.startService();
    kafkaCluster.createTopic(topicName);
    produceService = new ScheduledThreadPoolExecutor(1);
  }

  private void startSendJsonDataToKafka() {
    KafkaProducer<String, String> producer = kafkaCluster.getProducer(topicName);
    AtomicInteger sendCount = new AtomicInteger(0);
    produceService.scheduleAtFixedRate(() -> {
      try {
	for (int i = 0; i < 5000; ++i) {
	  String record = constructARecord(sendCount.getAndIncrement());
	  producer.send(new ProducerRecord(topicName, record));
	}
      } catch (Exception e) {
	LOG.error("failed to send a record");
      } finally {
	LOG.info(">>> kafka produce count: {}", sendCount.get());
      }
    }, 0, 1, TimeUnit.SECONDS);
  }

  private void startSendPbDataToKafka(BitSailConfiguration configuration) throws Exception {
    PbBytesParser parser = new PbBytesParser(configuration);
    List<Descriptors.FieldDescriptor> fields = parser.getDescriptor().getFields();
    Descriptors.Descriptor type = parser.getDescriptor();

    KafkaProducer<String, byte[]> producer = new KafkaProducer<>(
	ImmutableMap.of(
	    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaCluster.getBootstrapServer(),
	    ProducerConfig.CLIENT_ID_CONFIG, "producer"
	),
	new StringSerializer(),
    	new ByteArraySerializer()
    );
    AtomicInteger sendCount = new AtomicInteger(0);
    produceService.scheduleAtFixedRate(() -> {
      try {
	for (int i = 0; i < 5000; ++i) {
	  DynamicMessage.Builder builder = DynamicMessage.newBuilder(type);
	  for (Descriptors.FieldDescriptor field : fields) {
	    switch (field.getJavaType()) {
	      case INT:
		builder.setField(field, i);
		break;
	      case LONG:
		builder.setField(field, (long) i);
		break;
	      case FLOAT:
		builder.setField(field, (float) i);
		break;
	      case DOUBLE:
		builder.setField(field, (double) i);
		break;
	      case BOOLEAN:
		builder.setField(field, i % 2 == 0);
		break;
	      case BYTE_STRING:
		builder.setField(field, ByteString.copyFrom(("bytes_" + i).getBytes()));
		break;
	      case MESSAGE:
	      case STRING:
	      case ENUM:
	      default:
		builder.setField(field, "text_" + i);
		break;
	    }
	  }
	  DynamicMessage message = builder.build();
	  producer.send(new ProducerRecord(topicName, message.toByteArray()));
	  sendCount.getAndIncrement();
	}
      } catch (Exception e) {
	LOG.error("failed to send a record");
      } finally {
	LOG.info(">>> kafka produce count: {}", sendCount.get());
      }
    }, 0, 1, TimeUnit.SECONDS);
  }

  @Test
  public void testKafkaSourceJsonFormat() throws Exception {
    startSendJsonDataToKafka();
    BitSailConfiguration configuration = JobConfUtils.fromClasspath("kafka_to_print.json");
    updateConfiguration(configuration);
    EmbeddedFlinkCluster.submitJob(configuration);
  }

  @Test
  public void testKafkaSourcePbFormat() throws Exception {
    BitSailConfiguration configuration = JobConfUtils.fromClasspath("kafka_to_print_pb_format.json");
    URI proto = KafkaSourceITCase.class.getClassLoader().getResource("kafka.fds").toURI();
    byte[] descriptor = IOUtils.toByteArray(new File(proto).toURI());

    String protoDescriptor = Base64.getEncoder().encodeToString(descriptor);
    configuration.set(RowParserOptions.PROTO_DESCRIPTOR, protoDescriptor);
    configuration.set(RowParserOptions.PROTO_CLASS_NAME, "ProtoTest");
    startSendPbDataToKafka(configuration);
    updateConfiguration(configuration);
    EmbeddedFlinkCluster.submitJob(configuration);
  }

  @Test
  public void testKafkaSourcePbFormatFullTypes() throws Exception {
    BitSailConfiguration configuration = JobConfUtils.fromClasspath("kafka_to_print_pb_format_full_types.json");
    URI proto = KafkaSourceITCase.class.getClassLoader().getResource("kafka_full_types.fds").toURI();
    byte[] descriptor = IOUtils.toByteArray(new File(proto).toURI());

    String protoDescriptor = Base64.getEncoder().encodeToString(descriptor);
    configuration.set(RowParserOptions.PROTO_DESCRIPTOR, protoDescriptor);
    configuration.set(RowParserOptions.PROTO_CLASS_NAME, "ProtoTest");
    startSendPbDataToKafka(configuration);
    updateConfiguration(configuration);
    EmbeddedFlinkCluster.submitJob(configuration);
  }

  protected void updateConfiguration(BitSailConfiguration jobConfiguration) {
    //  jobConfiguration.set(FakeReaderOptions.TOTAL_COUNT, TOTAL_SEND_COUNT);

    Map<String, String> properties = Maps.newHashMap();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaCluster.getBootstrapServer());
    properties.put("topic", topicName);
    jobConfiguration.set("job.reader.connector.connector", properties);
  }

  @After
  public void after() {
    produceService.shutdown();
    kafkaCluster.stopService();
  }

}