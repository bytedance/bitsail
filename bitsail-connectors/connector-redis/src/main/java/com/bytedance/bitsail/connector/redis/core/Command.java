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

package com.bytedance.bitsail.connector.redis.core;

import com.bytedance.bitsail.connector.redis.core.jedis.JedisCommand;
import com.bytedance.bitsail.connector.redis.core.jedis.JedisCommandDescription;

import com.alibaba.fastjson.JSON;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;

@Data
public class Command {
  private JedisCommand jedisCommand;
  private byte[] key;
  private byte[] hashField;
  private double score;
  private byte[] value;
  private Map<byte[], byte[]> hash;
  private int ttlInSeconds;

  public Command(JedisCommandDescription commandDescription, byte[] key, byte[] hashField, byte[] value) {
    this(commandDescription, key, value);
    this.hashField = hashField;
  }

  public Command(JedisCommandDescription commandDescription, byte[] key, Map<byte[], byte[]> hash) {
    this.jedisCommand = commandDescription.getJedisCommand();
    this.key = key;
    this.hash = hash;
    this.ttlInSeconds = commandDescription.getAdditionalTTL() == null ? 0 : commandDescription.getAdditionalTTL();
  }

  public Command(JedisCommandDescription commandDescription, byte[] key, double score, byte[] value) {
    this(commandDescription, key, value);
    this.score = score;
  }

  public Command(JedisCommandDescription commandDescription, byte[] key, byte[] value) {
    this.jedisCommand = commandDescription.getJedisCommand();
    this.key = key;
    this.value = value;
    this.ttlInSeconds = commandDescription.getAdditionalTTL() == null ? 0 : commandDescription.getAdditionalTTL();
  }

  public String print() {
    switch (jedisCommand) {
      case SET:
      case SETEX:
        return new String(key);
      case SADD:
      case ZADD:
        return new String(key) + ":" + new String(value);
      case HSET:
        return new String(key) + ":" + new String(hashField);
      case HMSET:
        return new String(key) + ":" + JSON.toJSONString(hash);
      default:
        return StringUtils.EMPTY;
    }
  }
}
