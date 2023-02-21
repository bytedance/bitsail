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

package com.bytedance.bitsail.connector.redis.core.jedis;

import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.exception.CommonErrorCode;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.common.typeinfo.BasicArrayTypeInfo;
import com.bytedance.bitsail.common.typeinfo.BasicTypeInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfo;
import com.bytedance.bitsail.connector.redis.core.Command;

import lombok.Getter;

import java.util.HashMap;
import java.util.Map;

import static com.bytedance.bitsail.common.exception.CommonErrorCode.CONVERT_NOT_SUPPORT;

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
  SADD(JedisDataType.SET) {
    @Override
    public Command applyCommand(Row record) {
      byte[] key = convertFieldToBytes(record.getField(0), typeInfos[0]);
      byte[] value = convertFieldToBytes(record.getField(1), typeInfos[1]);
      return new Command(this, key, value, additionalTTL);
    }
  },

  /**
   * Set key to hold the string value. If key already holds a value,
   * it is overwritten, regardless of its type.
   */
  SET(JedisDataType.STRING) {
    @Override
    public Command applyCommand(Row record) {
      byte[] key = convertFieldToBytes(record.getField(0), typeInfos[0]);
      byte[] value = convertFieldToBytes(record.getField(1), typeInfos[1]);
      return new Command(this, key, value, additionalTTL);
    }
  },

  /**
   * Set key to hold the string value, with a time to live (TTL). If key already holds a value,
   * it is overwritten, regardless of its type.
   */
  SETEX(JedisDataType.STRING) {
    @Override
    public Command applyCommand(Row record) {
      byte[] key = convertFieldToBytes(record.getField(0), typeInfos[0]);
      byte[] value = convertFieldToBytes(record.getField(1), typeInfos[1]);
      return new Command(this, key, value, additionalTTL);
    }
  },

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
  ZADD(JedisDataType.SORTED_SET) {
    @Override
    public Command applyCommand(Row record) {
      // sorted set
      byte[] key = convertFieldToBytes(record.getField(0), typeInfos[0]);
      if (key.length == 0) {
        key = additionalKey.getBytes();
      }
      double score = parseScoreFromBytes(convertFieldToBytes(record.getField(1), typeInfos[1]));
      byte[] value = convertFieldToBytes(record.getField(2), typeInfos[2]);
      return new Command(this, key, score, value, additionalTTL);
    }
  },

  /**
   * todo: support this operation.
   * Removes the specified members from the sorted set stored at key.
   */
  ZREM(JedisDataType.SORTED_SET),

  /**
   * Sets field in the hash stored at key to value. If key does not exist,
   * a new key holding a hash is created. If field already exists in the hash, it is overwritten.
   */
  HSET(JedisDataType.HASH) {
    @Override
    public Command applyCommand(Row record) {
      // hash
      byte[] key = convertFieldToBytes(record.getField(0), typeInfos[0]);
      if (key.length == 0) {
        key = additionalKey.getBytes();
      }
      byte[] hashKey = convertFieldToBytes(record.getField(1), typeInfos[1]);
      byte[] value = convertFieldToBytes(record.getField(2), typeInfos[2]);
      return new Command(this, key, hashKey, value, additionalTTL);
    }
  },

  /**
   * Support an upstream row contain one key and multiple pairs,and it unlimited number of fields.
   */
  HMSET(JedisDataType.MHASH) {
    @Override
    public Command applyCommand(Row record) {
      // multi-hash
      byte[] key = convertFieldToBytes(record.getField(0), typeInfos[0]);
      int columnSize = record.getArity();
      if ((columnSize - 1) % 2 != 0) {
        throw new BitSailException(CONVERT_NOT_SUPPORT, "Inconsistent data entry.");
      }
      Map<byte[], byte[]> map = new HashMap<>((columnSize - 1) / 2);
      for (int idx = 1; idx < columnSize; idx += 2) {
        map.put(convertFieldToBytes(record.getField(idx), typeInfos[idx]),
            convertFieldToBytes(record.getField(idx + 1), typeInfos[idx + 1]));
      }
      return new Command(this, key, map, additionalTTL);
    }
  };

  /**
   * The {@link JedisDataType} this command belongs to.
   */
  @Getter
  private final JedisDataType jedisDataType;

  /**
   * This additional key is needed for the group {@link JedisDataType#HASH} and {@link JedisDataType#SORTED_SET}.
   * Other {@link JedisDataType} works only with two variable i.e. name of the list and value to be added.
   * But for {@link JedisDataType#HASH} and {@link JedisDataType#SORTED_SET} we need three variables.
   * <p>For {@link JedisDataType#HASH} we need hash name, hash key and element. Its possible to use TTL.
   * {@link #getAdditionalKey()} used as hash name for {@link JedisDataType#HASH}
   * <p>For {@link JedisDataType#SORTED_SET} we need set name, the element and it's score.
   * {@link #getAdditionalKey()} used as set name for {@link JedisDataType#SORTED_SET}
   */
  @Getter
  private static String additionalKey;

  /**
   * This additional key is optional for the group {@link JedisDataType#HASH}, required for {@link JedisCommand#SETEX}.
   * For the other types and commands, its not used.
   * <p>For {@link JedisDataType#HASH} we need hash name, hash key and element. Its possible to use TTL.
   * {@link #getAdditionalTTL()} used as time to live (TTL) for {@link JedisDataType#HASH}
   * <p>For {@link JedisCommand#SETEX}, we need key, value and time to live (TTL).
   */
  @Getter
  private static Integer additionalTTL;

  private static TypeInfo<?>[] typeInfos;

  JedisCommand(JedisDataType jedisDataType) {
    this.jedisDataType = jedisDataType;
  }

  /**
   * initialize {@link JedisCommand}.
   * For {@link JedisDataType#HASH} and {@link JedisDataType#SORTED_SET} data types, {@code additionalKey} is required.
   * For {@link JedisCommand#SETEX} command, {@code additionalTTL} is required.
   * In both cases, if the respective variables are not provided, it throws an {@link IllegalArgumentException}
   *
   * @param additionalKey additional key for Hash data type
   * @param additionalTTL additional TTL optional for Hash data type
   * @param typeInfos  the type information
   */
  public JedisCommand initialize(String additionalKey, Integer additionalTTL, TypeInfo<?>[] typeInfos) {
    JedisCommand.additionalKey = additionalKey;
    JedisCommand.additionalTTL = additionalTTL;
    JedisCommand.typeInfos = typeInfos;

    if (this.jedisDataType == JedisDataType.HASH ||
        this.jedisDataType == JedisDataType.MHASH ||
        this.jedisDataType == JedisDataType.SORTED_SET) {
      if (additionalKey == null) {
        throw new IllegalArgumentException("Hash and Sorted Set should have additional key");
      }
    }

    if (this.equals(JedisCommand.SETEX)) {
      if (additionalTTL == null) {
        throw new IllegalArgumentException("SETEX command should have time to live (TTL)");
      }
    }
    return this;
  }

  /**
   * @param record BitSail Row record
   * @return Command used in the job.
   */
  public Command applyCommand(Row record) {
    throw new UnsupportedOperationException();
  }

  private static double parseScoreFromBytes(byte[] scoreInBytes) throws BitSailException {
    try {
      return Double.parseDouble(new String(scoreInBytes));
    } catch (NumberFormatException exception) {
      throw new BitSailException(CommonErrorCode.CONVERT_NOT_SUPPORT,
          String.format("The score can't convert to double. And the score is %s.", new String(scoreInBytes)));
    }
  }

  private static byte[] convertFieldToBytes(Object field, TypeInfo<?> typeInfo) {
    if (typeInfo instanceof BasicTypeInfo) {
      return String.valueOf(field).getBytes();
    } else if (BasicArrayTypeInfo.BINARY_TYPE_INFO.getTypeClass() == typeInfo.getTypeClass()) {
      return (byte[]) field;
    }
    throw BitSailException.asBitSailException(CommonErrorCode.UNSUPPORTED_COLUMN_TYPE,
        typeInfo.getTypeClass().getName() + " is not supported in redis writer yet.");
  }
}
