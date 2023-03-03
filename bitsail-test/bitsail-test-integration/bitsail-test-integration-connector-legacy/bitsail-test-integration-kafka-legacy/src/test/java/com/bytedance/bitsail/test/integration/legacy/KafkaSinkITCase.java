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

package com.bytedance.bitsail.test.integration.legacy;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.connector.legacy.kafka.option.KafkaWriterOptions;
import com.bytedance.bitsail.test.integration.AbstractIntegrationTest;
import com.bytedance.bitsail.test.integration.legacy.container.KafkaCluster;
import com.bytedance.bitsail.test.integration.utils.JobConfUtils;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class KafkaSinkITCase extends AbstractIntegrationTest {
  private static final int TOTAL_SEND_COUNT = 300;
  private final String topicName = "testTopic";

  private final KafkaCluster kafkaCluster = new KafkaCluster();

  public static void checkByConsumer(KafkaCluster kafkaCluster, String topicName, int totalSendCount) {
    List<ConsumerRecord<?, ?>> records = kafkaCluster.consumerTopic(topicName, totalSendCount, 100);
    Assert.assertEquals(totalSendCount, records.size());
  }

  @Before
  public void before() {
    kafkaCluster.startService();
    kafkaCluster.createTopic(topicName);
  }

  @Test
  public void testKafkaOutputFormat() throws Exception {
    BitSailConfiguration configuration = JobConfUtils.fromClasspath("fake_to_kafka.json");
    updateConfiguration(configuration);
    submitJob(configuration);
    checkByConsumer(kafkaCluster, topicName, TOTAL_SEND_COUNT);
  }

  @After
  public void after() {
    kafkaCluster.stopService();
  }

  protected void updateConfiguration(BitSailConfiguration jobConfiguration) {
    jobConfiguration.set(KafkaWriterOptions.KAFKA_SERVERS, KafkaCluster.getBootstrapServer());
    jobConfiguration.set(KafkaWriterOptions.TOPIC_NAME, topicName);
  }
}
