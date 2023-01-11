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

import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.exception.CommonErrorCode;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.common.typeinfo.BasicArrayTypeInfo;
import com.bytedance.bitsail.common.typeinfo.BasicTypeInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfo;
import com.bytedance.bitsail.connector.redis.core.jedis.JedisCommandDescription;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static com.bytedance.bitsail.common.exception.CommonErrorCode.CONVERT_NOT_SUPPORT;

public class CommandFactory implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(CommandFactory.class);

  private final JedisCommandDescription commandDescription;

  private final TypeInfo<?>[] typeInfos;

  public static CommandFactory createCommandFactory(JedisCommandDescription commandDescription, TypeInfo<?>[] typeInfos) {
    return new CommandFactory(commandDescription, typeInfos);
  }

  private CommandFactory(JedisCommandDescription commandDescription, TypeInfo<?>[] typeInfos) {
    this.commandDescription = commandDescription;
    this.typeInfos = typeInfos;
  }

  /**
   * @param record BitSail Row record
   * @return Command used in the job.
   */
  public Command getCommand(Row record) {
    byte[] key = convertFieldToBytes(record.getField(0), typeInfos[0]);
    byte[] value;
    switch (commandDescription.getJedisCommand()) {
      case ZADD:
        // sorted set
        if (key.length == 0) {
          key = commandDescription.getAdditionalKey().getBytes();
        }
        double score = parseScoreFromBytes(convertFieldToBytes(record.getField(1), typeInfos[1]));
        value = convertFieldToBytes(record.getField(2), typeInfos[2]);
        return new Command(commandDescription, key, score, value);
      case HSET:
        // hash
        if (key.length == 0) {
          key = commandDescription.getAdditionalKey().getBytes();
        }
        byte[] hashKey = convertFieldToBytes(record.getField(1), typeInfos[1]);
        value = convertFieldToBytes(record.getField(2), typeInfos[2]);
        return new Command(commandDescription, key, hashKey, value);
      case HMSET:
        // hmset
        int columnSize = record.getArity();
        if ((columnSize - 1) % 2 != 0) {
          throw new BitSailException(CONVERT_NOT_SUPPORT, "Inconsistent data entry.");
        }
        Map<byte[], byte[]> map = new HashMap<>((columnSize - 1) / 2);
        for (int idx = 1; idx < columnSize; idx += 2) {
          map.put(convertFieldToBytes(record.getField(idx), typeInfos[idx]),
              convertFieldToBytes(record.getField(idx + 1), typeInfos[idx + 1]));
        }
        return new Command(commandDescription, key, map);
      default:
        // set and string
        value = convertFieldToBytes(record.getField(1), typeInfos[1]);
        return new Command(commandDescription, key, value);
    }
  }

  private double parseScoreFromBytes(byte[] scoreInBytes) throws BitSailException {
    try {
      return Double.parseDouble(new String(scoreInBytes));
    } catch (NumberFormatException exception) {
      throw new BitSailException(CommonErrorCode.CONVERT_NOT_SUPPORT,
          String.format("The score can't convert to double. And the score is %s.", new String(scoreInBytes)));
    }
  }

  private byte[] convertFieldToBytes(Object field, TypeInfo<?> typeInfo) {
    if (typeInfo instanceof BasicTypeInfo) {
      return String.valueOf(field).getBytes();
    } else if (BasicArrayTypeInfo.BINARY_TYPE_INFO.getTypeClass() == typeInfo.getTypeClass()) {
      return (byte[]) field;
    }
    throw BitSailException.asBitSailException(CommonErrorCode.UNSUPPORTED_COLUMN_TYPE,
        typeInfo.getTypeClass().getName() + " is not supported in redis writer yet.");
  }
}
