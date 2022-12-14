/*
 * Copyright 2022-present, Bytedance Ltd.
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

package com.bytedance.bitsail.connector.redis.config;

import com.bytedance.bitsail.connector.redis.core.jedis.JedisCommandDescription;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import org.apache.flink.api.java.typeutils.RowTypeInfo;

import java.io.Serializable;

@Builder
@AllArgsConstructor
@Data
public class RedisOptions implements Serializable {
  private static final long serialVersionUID = 1865789744864217944L;

  private String redisHost;

  private int redisPort;

  private String redisPassword;

  private int timeout;

  /**
   * Expiring times in seconds.
   */
  private int ttlInSeconds;

  /**
   * The number of record per batch.
   */
  private int batchInterval;

  /**
   * Command used in the job.
   */
  private JedisCommandDescription commandDescription;

  /**
   * Number of columns to send in each record.
   */
  private int columnSize;

  private RowTypeInfo rowTypeInfo;

  /**
   * Log sample every "logSampleInterval" batch.
   */
  private int logSampleInterval;

  /**
   * Complex type command with ttl.
   */
  private boolean complexTypeWithTtl;

  /**
   * Retryer retry count
   */
  private int maxAttemptCount;
}
