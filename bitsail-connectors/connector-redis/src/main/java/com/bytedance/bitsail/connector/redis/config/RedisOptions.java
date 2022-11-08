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
   * Also BatchSize, Batch send by pipeline after 'batchInterval' records.
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
   * Log interval of pipelines, Send log every 'logSampleInterval' times flush (Each flush corresponds to one pipeline)
   */
  private int logSampleInterval;

  /**
   * Complex type command with ttl.
   */
  private boolean complexTypeWithTtl;
}
