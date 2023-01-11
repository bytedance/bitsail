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

package com.bytedance.bitsail.connector.redis.core.jedis;

import lombok.Getter;

/**
 * All available commands for Redis. Each command belongs to a {@link JedisDataType} group.
 */
public enum JedisCommand {

  /**
   * Unsupported because it's not idempotent.
   * Insert the specified value at the head of the list stored at key.
   * If key does not exist, it is created as empty list before performing the push operations.
   */
  LPUSH(JedisDataType.LIST),

  /**
   * Unsupported because it's not idempotent.
   * Insert the specified value at the tail of the list stored at key.
   * If key does not exist, it is created as empty list before performing the push operation.
   */
  RPUSH(JedisDataType.LIST),

  /**
   * Add the specified member to the set stored at key.
   * Specified member that is already a member of this set is ignored.
   */
  SADD(JedisDataType.SET),

  /**
   * Set key to hold the string value. If key already holds a value,
   * it is overwritten, regardless of its type.
   */
  SET(JedisDataType.STRING),

  /**
   * Set key to hold the string value, with a time to live (TTL). If key already holds a value,
   * it is overwritten, regardless of its type.
   */
  SETEX(JedisDataType.STRING),

  /**
   * todo: support this operation.
   * Adds the element to the HyperLogLog data structure stored at the variable name specified as first argument.
   */
  PFADD(JedisDataType.HYPER_LOG_LOG),

  /**
   * todo: support this operation.
   * Posts a message to the given channel.
   */
  PUBLISH(JedisDataType.PUBSUB),

  /**
   * Adds the specified members with the specified score to the sorted set stored at key.
   */
  ZADD(JedisDataType.SORTED_SET),

  /**
   * todo: support this operation.
   * Removes the specified members from the sorted set stored at key.
   */
  ZREM(JedisDataType.SORTED_SET),

  /**
   * Sets field in the hash stored at key to value. If key does not exist,
   * a new key holding a hash is created. If field already exists in the hash, it is overwritten.
   */
  HSET(JedisDataType.HASH),

  /**
   * Support an upstream row contain one key and multiple pairs,and it unlimited number of fields.
   */
  HMSET(JedisDataType.SORTED_SET);

  /**
   * The {@link JedisDataType} this command belongs to.
   */
  @Getter
  private final JedisDataType jedisDataType;

  JedisCommand(JedisDataType jedisDataType) {
    this.jedisDataType = jedisDataType;
  }
}
