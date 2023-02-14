/*
 * Copyright 2022-2023 Bytedance Ltd. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bytedance.bitsail.test.e2e.datasource;

import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.exception.CommonErrorCode;
import com.bytedance.bitsail.common.model.ColumnInfo;
import com.bytedance.bitsail.common.option.CommonOptions;
import com.bytedance.bitsail.common.option.ReaderOptions;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.common.typeinfo.TypeInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfoUtils;
import com.bytedance.bitsail.common.util.JsonSerializer;
import com.bytedance.bitsail.connector.rocketmq.option.RocketMQSourceOptions;
import com.bytedance.bitsail.connector.rocketmq.source.RocketMQSource;
import com.bytedance.bitsail.test.e2e.datasource.util.RowGenerator;
import com.bytedance.bitsail.test.e2e.executor.AbstractExecutor;

import com.alibaba.dcm.DnsCacheManipulator;
import com.google.common.collect.Lists;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class RocketMQDataSource extends AbstractDataSource {
  private static final Logger LOG = LoggerFactory.getLogger(RocketMQDataSource.class);

  private static final DockerImageName ROCKET_MQ_DOCKER_IMAGE = DockerImageName
      .parse("apache/rocketmq:4.9.4");

  private static final String PRODUCE_GROUP = "bitsail";
  private static final String TOPIC_NAME = "test_topic";

  private GenericContainer<?> nameServ;
  private GenericContainer<?> brokerServ;

  private List<ColumnInfo> columnInfos;
  private RowGenerator rowGenerator;

  private boolean hasFillData = false;

  @Override
  public String getContainerName() {
    return "data-source-rocketmq";
  }

  @Override
  public void initNetwork(Network executorNetwork) {
    this.network = executorNetwork;
  }

  @Override
  public void configure(BitSailConfiguration dataSourceConf) {
    RocketMQSource rocketMQSource = new RocketMQSource();
    this.columnInfos = dataSourceConf.get(ReaderOptions.BaseReaderOptions.COLUMNS);
    TypeInfo<?>[] producerTypes = TypeInfoUtils.getTypeInfos(
        rocketMQSource.createTypeInfoConverter(),
        columnInfos);
    this.rowGenerator = new RowGenerator(producerTypes);
    rowGenerator.init();
  }

  @Override
  public boolean accept(BitSailConfiguration jobConf, Role role) {
    String readerClass = jobConf.get(ReaderOptions.READER_CLASS);
    if (role == Role.SOURCE) {
      return RocketMQSource.class.getName().equals(readerClass);
    }
    return false;
  }

  @Override
  public void modifyJobConf(BitSailConfiguration jobConf) {
    jobConf.set(RocketMQSourceOptions.CLUSTER, "nameserv:9876");
    jobConf.set(RocketMQSourceOptions.TOPIC, TOPIC_NAME);
    jobConf.set(RocketMQSourceOptions.CONSUMER_OFFSET_MODE, "earliest");
    jobConf.set(RocketMQSourceOptions.CONSUMER_GROUP, PRODUCE_GROUP);

    jobConf.set(CommonOptions.JOB_TYPE, "batch");
  }

  @SuppressWarnings("checkstyle:MagicNumber")
  @Override
  public void start() {
    // init name server
    nameServ = new GenericContainer<>(ROCKET_MQ_DOCKER_IMAGE)
        .withStartupTimeout(Duration.of(5, ChronoUnit.MINUTES))
        .withCommand("sh mqnamesrv")
        .withNetwork(network)
        .withNetworkAliases("nameserv")
        .withExposedPorts()
        .withLogConsumer(new Slf4jLogConsumer(LOG))
        .waitingFor(new LogMessageWaitStrategy()
            .withRegEx(".*The Name Server boot success.*")
            .withStartupTimeout(Duration.ofMinutes(2)));
    ArrayList<String> portBindings = new ArrayList<>();
    portBindings.add("9876:9876");
    nameServ.setPortBindings(portBindings);

    // init broker
    brokerServ = new GenericContainer<>(ROCKET_MQ_DOCKER_IMAGE)
        .withCommand("sh mqbroker -n nameserv:9876 -c /tmp/broker.conf")
        .withStartupTimeout(Duration.of(5, ChronoUnit.MINUTES))
        .withNetwork(network)
        .withNetworkAliases("broker")
        .withLogConsumer(new Slf4jLogConsumer(LOG))
        .withExposedPorts()
        .dependsOn(nameServ)
        .waitingFor(new LogMessageWaitStrategy()
            .withRegEx(".*The broker.*boot success.*")
            .withStartupTimeout(Duration.ofMinutes(2)));
    ArrayList<String> portBindingsBroker = new ArrayList<>();
    portBindingsBroker.add("10911:10911");
    brokerServ.setPortBindings(portBindingsBroker);

    brokerServ.addFileSystemBind(MountableFile.forClasspathResource("/broker.conf")
            .getFilesystemPath(),
        "/tmp/broker.conf",
        BindMode.READ_ONLY);

    Startables.deepStart(Stream.of(nameServ)).join();
    Startables.deepStart(Stream.of(brokerServ)).join();

    LOG.info("RocketMQ cluster starts!");
  }

  @SuppressWarnings("checkstyle:MagicNumber")
  @Override
  public void fillData(AbstractExecutor ignored) {
    if (hasFillData) {
      return;
    }
    hasFillData = true;

    synchronized (RocketMQSource.class) {
      DnsCacheManipulator.setDnsCache("broker", "127.0.0.1");
    }

    // create producer
    DefaultMQProducer producer = new DefaultMQProducer(PRODUCE_GROUP);
    producer.setNamesrvAddr("localhost:9876");
    try {
      producer.start();
    } catch (Exception e) {
      throw BitSailException.asBitSailException(CommonErrorCode.RUNTIME_ERROR,
          "Failed to start up a producer.", e);
    }
    producer.setSendMsgTimeout(5 * 1000);
    producer.setMqClientApiTimeout(5 * 1000);

    // produce
    for (int batchCount = 0; batchCount < 10; ++batchCount) {
      List<Message> messages = Lists.newArrayList();
      for (int i = 0; i < 100; ++i) {
        messages.add(new Message(TOPIC_NAME, fakeJsonObject()));
      }
      try {
        producer.send(messages);
        TimeUnit.MILLISECONDS.sleep(500);
      } catch (Exception e) {
        throw BitSailException.asBitSailException(CommonErrorCode.RUNTIME_ERROR,
            "Failed to produce data.", e);
      }
    }
    producer.shutdown();
    LOG.info("Successfully produce 1000 records to rocket mq.");

    synchronized (RocketMQSource.class) {
      DnsCacheManipulator.removeDnsCache("broker");
    }
  }

  @Override
  public void close() throws IOException {
    if (Objects.nonNull(nameServ)) {
      nameServ.close();
      nameServ = null;
    }
    if (Objects.nonNull(brokerServ)) {
      brokerServ.close();
      brokerServ = null;
    }
    super.close();
  }

  private byte[] fakeJsonObject() {
    Row row = rowGenerator.next();

    Map<String, Object> content = new HashMap<>();
    for (int i = 0; i < columnInfos.size(); ++i) {
      String name = columnInfos.get(i).getName();
      Object value = row.getField(i);
      content.put(name, value);
    }

    return JsonSerializer.serialize(content).getBytes();
  }
}
