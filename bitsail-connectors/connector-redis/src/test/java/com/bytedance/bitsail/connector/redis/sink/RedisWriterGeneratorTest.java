

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
package com.bytedance.bitsail.connector.redis.sink;

import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.exception.CommonErrorCode;
import com.bytedance.bitsail.connector.redis.core.jedis.JedisCommand;
import com.bytedance.bitsail.connector.redis.core.jedis.JedisCommandDescription;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class RedisWriterGeneratorTest {


  @Test
  public void initRedisCommandDescriptionTest() {
    RedisWriterGenerator redisWriterGenerator = new RedisWriterGenerator();
    // ttl < 0
    String redisType = "string";
    int ttl = -1;
    String additionalKey = "dts_test";
    JedisCommandDescription redisCommandDescriptionWithoutTtl = redisWriterGenerator.initJedisCommandDescription(redisType, ttl, additionalKey);
    assertEquals(JedisCommand.SET, redisCommandDescriptionWithoutTtl.getJedisCommand());
    assertNull(redisCommandDescriptionWithoutTtl.getAdditionalTTL());
    assertEquals(additionalKey, redisCommandDescriptionWithoutTtl.getAdditionalKey());

    // ttl > 0
    ttl = 1;
    JedisCommandDescription redisCommandDescriptionWithTtl = redisWriterGenerator.initJedisCommandDescription(redisType, ttl, additionalKey);
    assertEquals(JedisCommand.SETEX, redisCommandDescriptionWithTtl.getJedisCommand());
    assertEquals((Integer) ttl, redisCommandDescriptionWithTtl.getAdditionalTTL());
    assertEquals(additionalKey, redisCommandDescriptionWithTtl.getAdditionalKey());

    // hash type
    redisType = "hash";
    JedisCommandDescription redisCommandDescriptionHashType = redisWriterGenerator.initJedisCommandDescription(redisType, ttl, additionalKey);
    assertEquals(JedisCommand.HSET, redisCommandDescriptionHashType.getJedisCommand());
    assertEquals(additionalKey, redisCommandDescriptionHashType.getAdditionalKey());

    // hash type without additional key, throw exception
    try {
      redisWriterGenerator.initJedisCommandDescription(redisType, ttl, null);
      throw new BitSailException(CommonErrorCode.CONFIG_ERROR, "Unit test error");
    } catch (IllegalArgumentException e) {
      assertEquals("Hash and Sorted Set should have additional key", e.getMessage());
    }
  }

}
