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

package com.bytedance.bitsail.connector.legacy.rocketmq.sink;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.connector.legacy.fake.option.FakeReaderOptions;
import com.bytedance.bitsail.connector.legacy.rocketmq.option.RocketMQWriterOptions;
import com.bytedance.bitsail.test.connector.test.EmbeddedFlinkCluster;
import com.bytedance.bitsail.test.connector.test.utils.JobConfUtils;

import org.junit.Ignore;
import org.junit.Test;

/**
 * Need to start rocket service first.
 */
@Ignore
public class RocketMQOutputFormatITCase {

  private static final int TOTAL_SEND_COUNT = 300;
  private static final String topicName = "test_topic";

  @Test
  public void testFakeToRocketMQ() throws Exception {
    BitSailConfiguration configuration = JobConfUtils.fromClasspath("fake_to_rocketmq.json");
    updateConfiguration(configuration);
    EmbeddedFlinkCluster.submitJob(configuration);
  }

  protected void updateConfiguration(BitSailConfiguration jobConfiguration) {
    jobConfiguration.set(FakeReaderOptions.TOTAL_COUNT, TOTAL_SEND_COUNT);
    jobConfiguration.set(RocketMQWriterOptions.NAME_SERVER_ADDRESS, "127.0.0.1:9876");
    jobConfiguration.set(RocketMQWriterOptions.TOPIC, topicName);
    jobConfiguration.set(RocketMQWriterOptions.PRODUCER_GROUP, "test_producer_group");
    jobConfiguration.set(RocketMQWriterOptions.TAG, "itcase_test");
    jobConfiguration.set(RocketMQWriterOptions.PARTITION_FIELDS, "id");
    jobConfiguration.set(RocketMQWriterOptions.ENABLE_BATCH_FLUSH, false);
  }
}
