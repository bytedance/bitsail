/*
 * Copyright 2022 Bytedance Ltd. and/or its affiliates.
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

package com.bytedance.bitsail.connector.redis.sink;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.option.CommonOptions;
import com.bytedance.bitsail.connector.fake.option.FakeReaderOptions;
import com.bytedance.bitsail.connector.redis.option.RedisWriterOptions;
import com.bytedance.bitsail.test.connector.test.EmbeddedFlinkCluster;
import com.bytedance.bitsail.test.connector.test.testcontainers.redis.RedisContainer;
import com.bytedance.bitsail.test.connector.test.utils.JobConfUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class RedisSinkITCase {
  private static final int TOTAL_COUNT = 300;
  private RedisContainer redisContainer;
  private String redisHost;
  private int redisPort;

  @Before
  public void initRedis() {
    redisContainer = new RedisContainer();
    redisContainer.start();
    redisHost = redisContainer.getHost();
    redisPort = redisContainer.getPort();
  }

  @After
  public void closeRedis() throws Exception {
    redisContainer.close();
  }

  @Test
  public void testBatch() throws Exception {
    BitSailConfiguration jobConfiguration = JobConfUtils.fromClasspath("fake_to_redis_hash.json");
    jobConfiguration.set(FakeReaderOptions.TOTAL_COUNT, TOTAL_COUNT);
    jobConfiguration.set(CommonOptions.JOB_TYPE, "batch");
    jobConfiguration.set(FakeReaderOptions.RATE, 1000);
    jobConfiguration.set(RedisWriterOptions.HOST, redisHost);
    jobConfiguration.set(RedisWriterOptions.PORT, redisPort);

    EmbeddedFlinkCluster.submitJob(jobConfiguration);

    Assert.assertEquals(TOTAL_COUNT, redisContainer.getKeyCount());
  }

  @Test
  public void testStreaming() throws Exception {
    BitSailConfiguration jobConfiguration = JobConfUtils.fromClasspath("fake_to_redis_hash.json");
    jobConfiguration.set(FakeReaderOptions.TOTAL_COUNT, TOTAL_COUNT);
    jobConfiguration.set(CommonOptions.JOB_TYPE, "streaming");
    jobConfiguration.set(CommonOptions.CheckPointOptions.CHECKPOINT_ENABLE, true);
    jobConfiguration.set(RedisWriterOptions.HOST, redisHost);
    jobConfiguration.set(RedisWriterOptions.PORT, redisPort);

    EmbeddedFlinkCluster.submitJob(jobConfiguration);

    Assert.assertEquals(TOTAL_COUNT, redisContainer.getKeyCount());
  }

}
