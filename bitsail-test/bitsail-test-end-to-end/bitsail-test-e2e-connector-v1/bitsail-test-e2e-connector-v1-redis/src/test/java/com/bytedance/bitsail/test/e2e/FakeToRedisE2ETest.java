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

package com.bytedance.bitsail.test.e2e;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.connector.fake.option.FakeReaderOptions;
import com.bytedance.bitsail.test.e2e.datasource.RedisDataSource;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.nio.file.Paths;

public class FakeToRedisE2ETest extends AbstractE2ETest {

  @Test
  public void testFakeToRedis() throws Exception {
    BitSailConfiguration jobConf = BitSailConfiguration.from(
        new File(Paths.get(getClass().getClassLoader()
            .getResource("fake_to_redis.json")
            .toURI()).toString()));
    jobConf.set(FakeReaderOptions.TOTAL_COUNT, 500);

    // Check if there are 500 keys in redis.
    submitJob(jobConf,
        "test_fake_to_redis",
        dataSource -> Assert.assertEquals(
            500,
            ((RedisDataSource) dataSource).getKeyCount()
        ));
  }
}
