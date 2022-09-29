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

package com.bytedance.bitsail.component.client.kafka;

import com.bytedance.bitsail.test.connector.test.testcontainers.kafka.KafkaCluster;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Callback;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

public class KafkaProducerTest {

  private static final int TOTAL_SEND_COUNT = 300;
  private final String topicName = "testTopic";

  private final KafkaCluster kafkaCluster = new KafkaCluster();
  private KafkaProducer producer;

  public void initKafkaCluster() {
    kafkaCluster.startService();
    kafkaCluster.createTopic(topicName);
    producer = new KafkaProducer(KafkaCluster.getBootstrapServer(), topicName);
  }

  @After
  public void closeKafkaCluster() {
    kafkaCluster.stopService();
  }

  @Test
  public void testSyncSendWithoutKey() {
    initKafkaCluster();
    for (int i = 0; i < TOTAL_SEND_COUNT; ++i) {
      producer.send(String.format("{\"name\":\"n%s\"}", i));
    }
    producer.close();
    checkValues();
  }

  @Test
  public void testAsyncSendWithKey() {
    initKafkaCluster();
    AtomicInteger respCount = new AtomicInteger(0);
    Callback callback = (metadata, e) -> {
      if (Objects.isNull(e)) {
        respCount.incrementAndGet();
      }
    };
    for (int i = 0; i < TOTAL_SEND_COUNT; ++i) {
      producer.send("key" + i, String.format("{\"name\":\"n%s\"}", i), callback);
    }
    producer.close();
    Assert.assertEquals(respCount.get(), TOTAL_SEND_COUNT);
    checkKeysAndValues();
  }

  @Test
  public void testSelectPartition() {
    int[] partitionList = new int[] {0, 1, 2, 3, 4, 5, 6, 7};
    String[] fields = new String[] {"a", "b", "c"};
    int expectedPartition = Arrays.stream(fields).map(String::hashCode).reduce(Integer::sum).orElse(0) % 8;
    Assert.assertEquals(expectedPartition, KafkaProducer.choosePartitionIdByFields(partitionList, fields));
  }

  private void checkValues() {
    List<ConsumerRecord<?, ?>> records = kafkaCluster.consumerTopic(topicName, TOTAL_SEND_COUNT, 100);
    Assert.assertEquals(TOTAL_SEND_COUNT, records.size());
    for (int i = 0; i < TOTAL_SEND_COUNT; ++i) {
      Assert.assertEquals(String.format("{\"name\":\"n%s\"}", i), records.get(i).value());
    }
  }

  private void checkKeysAndValues() {
    List<ConsumerRecord<?, ?>> records = kafkaCluster.consumerTopic(topicName, TOTAL_SEND_COUNT, 100);
    Assert.assertEquals(TOTAL_SEND_COUNT, records.size());
    for (int i = 0; i < TOTAL_SEND_COUNT; ++i) {
      Assert.assertEquals("key" + i, records.get(i).key());
      Assert.assertEquals(String.format("{\"name\":\"n%s\"}", i), records.get(i).value());
    }
  }
}
