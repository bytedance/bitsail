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

package com.bytedance.bitsail.test.integration.legacy.redis;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.connector.legacy.fake.option.FakeReaderOptions;
import com.bytedance.bitsail.connector.legacy.redis.option.RedisWriterOptions;
import com.bytedance.bitsail.test.integration.AbstractIntegrationTest;
import com.bytedance.bitsail.test.integration.legacy.redis.container.RedisContainer;
import com.bytedance.bitsail.test.integration.utils.JobConfUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class RedisOutputFormatITCase extends AbstractIntegrationTest {

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

  @Test
  public void testRedisOutputFormat() throws Exception {
    BitSailConfiguration configuration = JobConfUtils.fromClasspath("fake_to_redis.json");
    updateConfiguration(configuration);
    submitJob(configuration);

    Assert.assertEquals(TOTAL_COUNT, redisContainer.getKeyCount());
  }

  protected void updateConfiguration(BitSailConfiguration jobConfiguration) {
    jobConfiguration.set(FakeReaderOptions.TOTAL_COUNT, TOTAL_COUNT);
    jobConfiguration.set(RedisWriterOptions.HOST, redisHost);
    jobConfiguration.set(RedisWriterOptions.PORT, redisPort);
  }

  @After
  public void clear() throws Exception {
    redisContainer.close();
  }
}
