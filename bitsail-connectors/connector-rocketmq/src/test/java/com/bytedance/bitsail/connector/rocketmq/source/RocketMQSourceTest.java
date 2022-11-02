/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.bytedance.bitsail.connector.rocketmq.source;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.util.JsonSerializer;
import com.bytedance.bitsail.test.connector.test.EmbeddedFlinkCluster;
import com.bytedance.bitsail.test.connector.test.utils.JobConfUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.SneakyThrows;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
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

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

public class RocketMQSourceTest extends EmbeddedFlinkCluster {
  private static final Logger LOG = LoggerFactory.getLogger(RocketMQSourceTest.class);

  private static final String ROCKETMQ_SOURCE_VERSION = "4.9.4";
  private static final DockerImageName ROCKET_MQ_DOCKER_IMAGE = DockerImageName
      .parse("apache/rocketmq:4.9.4")
      .withTag(ROCKETMQ_SOURCE_VERSION);
  private static final Network NETWORK = Network.newNetwork();
  private GenericContainer<?> nameServ;
  private GenericContainer<?> brokerServ;
  private DefaultMQProducer producer;

  private static final String DEFAULT_PRODUCE_GROUP = "bitsail";
  private static final String DEFAULT_TOPIC = "TBW102";
  private static final int DEFAULT_QUEUE_NUMBER = 5;

  @Before
  public void before() throws MQClientException {
    nameServ = new GenericContainer<>(ROCKET_MQ_DOCKER_IMAGE)
        .withStartupTimeout(Duration.of(5, ChronoUnit.MINUTES))
        .withCommand("sh mqnamesrv")
        .withNetwork(NETWORK)
        .withNetworkAliases("nameserv")
        .withExposedPorts()
        .withLogConsumer(new Slf4jLogConsumer(LOG))
        .waitingFor(new LogMessageWaitStrategy()
            .withRegEx(".*boot success.*")
            .withStartupTimeout(Duration.ofMinutes(2)));

    ArrayList<String> portBindings = new ArrayList<>();
    portBindings.add("9876:9876");
    nameServ.setPortBindings(portBindings);

    brokerServ = new GenericContainer<>(ROCKET_MQ_DOCKER_IMAGE)
        .withCommand("sh mqbroker -n nameserv:9876 -c /tmp/broker.conf")
        .withStartupTimeout(Duration.of(5, ChronoUnit.MINUTES))
        .withNetwork(NETWORK)
        .withNetworkAliases("broker")
        .withLogConsumer(new Slf4jLogConsumer(LOG))
        .withExposedPorts()
        .dependsOn(nameServ);
        //.waitingFor(new LogMessageWaitStrategy()
        //    .withRegEx(".*boot success.*")
        //    .withStartupTimeout(Duration.ofMinutes(2)));
    ArrayList<String> portBindingsBroker = new ArrayList<>();
    portBindingsBroker.add("10911:10911");
    brokerServ.setPortBindings(portBindingsBroker);

    brokerServ.addFileSystemBind(MountableFile.forClasspathResource("/broker.conf")
            .getFilesystemPath(),
        "/tmp/broker.conf",
        BindMode.READ_ONLY);
    Startables.deepStart(Stream.of(nameServ)).join();
    Startables.deepStart(Stream.of(brokerServ)).join();

    prepareRocketMQ();
  }

  private void prepareRocketMQ() throws MQClientException {
    try {
      producer = new DefaultMQProducer(DEFAULT_PRODUCE_GROUP);
      producer.setNamesrvAddr("localhost:9876");
      producer.start();
      producer.setSendMsgTimeout(5 * 1000);
      producer.setMqClientApiTimeout(5 * 1000);
      List<MessageQueue> messageQueues = producer.fetchPublishMessageQueues(DEFAULT_TOPIC);
      new Thread(new Runnable() {
        @SneakyThrows
        @Override
        public void run() {
          List<Message> messages = Lists.newArrayList();
          for (int i = 0; i < 100; i++) {
            messages.add(new Message(DEFAULT_TOPIC, fakeJsonObject(i)));
          }
          producer.send(messages);
        }
      });
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private static byte[] fakeJsonObject(int index) {
    Map<Object, Object> demo = Maps.newHashMap();
    demo.put("id", index);
    return JsonSerializer.serialize(demo).getBytes();
  }

  @After
  public void after() {
    if (Objects.nonNull(nameServ)) {
      nameServ.close();
    }
    if (Objects.nonNull(brokerServ)) {
      brokerServ.close();
    }
  }

  @Test
  public void testBoundednessRocketMQSource() throws Exception {
    BitSailConfiguration jobConf = JobConfUtils.fromClasspath("bitsail_rocketmq_print.json");
    EmbeddedFlinkCluster.submitJob(jobConf);
  }

}