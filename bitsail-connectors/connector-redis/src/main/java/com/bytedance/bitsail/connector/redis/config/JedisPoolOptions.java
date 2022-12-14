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

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.io.Serializable;

@Builder
@AllArgsConstructor
@Data
public class JedisPoolOptions implements Serializable {
  /**
   * Jedis pool max total connection
   */
  private int maxTotalConnection;

  /**
   * Jedis pool max idle connection
   */
  private int maxIdleConnection;

  /**
   * Jedis pool min idle connection
   */
  private int minIdleConnection;

  /**
   * Jedis pool max wait time in millis
   */
  private int maxWaitTimeInMillis;
}
