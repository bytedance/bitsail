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

import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.exception.CommonErrorCode;
import com.bytedance.bitsail.connector.redis.core.jedis.JedisCommand;
import com.bytedance.bitsail.connector.redis.core.jedis.JedisCommandDescription;
import com.bytedance.bitsail.test.connector.test.utils.JobConfUtils;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class RedisSinkTest {
  @Test
  public void initRedisCommandDescriptionTest() throws Exception {
    BitSailConfiguration bitSailConfiguration = JobConfUtils.fromClasspath("fake_to_redis_hash.json");
    RedisWriter<Object> redisWriter = new RedisWriter<>(bitSailConfiguration);

    // ttl < 0
    String redisType = "string";
    int ttl = -1;
    String additionalKey = "bitsail_test";
    JedisCommandDescription redisCommandDescriptionWithoutTtl = redisWriter.initJedisCommandDescription(redisType, ttl, additionalKey);
    assertEquals(JedisCommand.SET, redisCommandDescriptionWithoutTtl.getJedisCommand());
    assertNull(redisCommandDescriptionWithoutTtl.getAdditionalTTL());
    assertEquals(additionalKey, redisCommandDescriptionWithoutTtl.getAdditionalKey());

    // ttl > 0
    ttl = 1;
    JedisCommandDescription redisCommandDescriptionWithTtl = redisWriter.initJedisCommandDescription(redisType, ttl, additionalKey);
    assertEquals(JedisCommand.SETEX, redisCommandDescriptionWithTtl.getJedisCommand());
    assertEquals((Integer) ttl, redisCommandDescriptionWithTtl.getAdditionalTTL());
    assertEquals(additionalKey, redisCommandDescriptionWithTtl.getAdditionalKey());

    // hash type
    redisType = "hash";
    JedisCommandDescription redisCommandDescriptionHashType = redisWriter.initJedisCommandDescription(redisType, ttl, additionalKey);
    assertEquals(JedisCommand.HSET, redisCommandDescriptionHashType.getJedisCommand());
    assertEquals(additionalKey, redisCommandDescriptionHashType.getAdditionalKey());

    // hash type without additional key, throw exception
    try {
      redisWriter.initJedisCommandDescription(redisType, ttl, null);
      throw new BitSailException(CommonErrorCode.CONFIG_ERROR, "Unit test error");
    } catch (IllegalArgumentException e) {
      assertEquals("Hash and Sorted Set should have additional key", e.getMessage());
    }
  }

}
