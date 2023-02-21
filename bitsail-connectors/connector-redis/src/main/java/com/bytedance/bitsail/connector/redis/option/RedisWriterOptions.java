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

package com.bytedance.bitsail.connector.redis.option;

import com.bytedance.bitsail.common.option.ConfigOption;
import com.bytedance.bitsail.common.option.WriterOptions;

import static com.bytedance.bitsail.common.option.ConfigOptions.key;
import static com.bytedance.bitsail.common.option.WriterOptions.WRITER_PREFIX;

public interface RedisWriterOptions extends WriterOptions.BaseWriterOptions {

  /**
   * Redis host
   */
  ConfigOption<String> HOST =
      key(WRITER_PREFIX + "redis_host")
          .noDefaultValue(String.class);

  /**
   * Redis port
   */
  ConfigOption<Integer> PORT =
      key(WRITER_PREFIX + "redis_port")
          .noDefaultValue(Integer.class);

  /**
   * Client & Connection & Read/Write timeout in ms.
   */
  ConfigOption<Integer> CLIENT_TIMEOUT_MS =
      key(WRITER_PREFIX + "client_timeout_ms")
          .defaultValue(60000);

  /**
   * TTL of inserted keys.
   * Default -1 means no ttl.
   */
  ConfigOption<Integer> TTL =
      key(WRITER_PREFIX + "ttl")
          .defaultValue(-1);

  /**
   * TTL unit.
   */
  ConfigOption<String> TTL_TYPE =
      key(WRITER_PREFIX + "ttl_type")
          .defaultValue("DAY");

  @SuppressWarnings("checkstyle:MagicNumber")
  ConfigOption<Integer> WRITE_BATCH_SIZE =
      key(WRITER_PREFIX + "write_batch_interval")
          .defaultValue(50)
          .withAlias(WRITER_PREFIX + "write_batch_interval");


  /**
   * Data type to insert into redis.
   */
  ConfigOption<String> REDIS_DATA_TYPE =
      key(WRITER_PREFIX + "redis_data_type")
          .defaultValue("string");

  /**
   * This additional key needed for hash and sorted set.
   * Other redis data type works only with two variable i.e. name of the list and value to be added.
   * But for hash and sorted set we need three variables.
   * <p>For hash we need hash name, hash key and element.
   * {@code additionalKey} used as hash name for hash when there are only 2 values from source.
   * <p>For sorted set we need set name, the element and it's score.
   * {@code additionalKey} used as set name for sorted set when there are only 2 values from source.
   */
  ConfigOption<String> ADDITIONAL_KEY =
      key(WRITER_PREFIX + "additional_key")
          .noDefaultValue(String.class);

  /**
   * Log sample interval.
   */
  ConfigOption<Integer> LOG_SAMPLE_INTERVAL =
      key(WRITER_PREFIX + "log_sample_interval")
          .defaultValue(256);

  /**
   * Jedis pool max total connection
   */
  ConfigOption<Integer> JEDIS_POOL_MAX_TOTAL_CONNECTIONS =
      key(WRITER_PREFIX + "connection_pool_max_total")
          .defaultValue(2);

  /**
   * Jedis pool max idle connection
   */
  ConfigOption<Integer> JEDIS_POOL_MAX_IDLE_CONNECTIONS =
      key(WRITER_PREFIX + "connection_pool_max_idle")
          .defaultValue(2);

  /**
   * Jedis pool min idle connection
   */
  ConfigOption<Integer> JEDIS_POOL_MIN_IDLE_CONNECTIONS =
      key(WRITER_PREFIX + "connection_pool_min_idle")
          .defaultValue(0);

  /**
   * Jedis pool max wait time in millis
   */
  ConfigOption<Integer> JEDIS_POOL_MAX_WAIT_TIME_IN_MILLIS =
      key(WRITER_PREFIX + "connection_pool_max_wait_time_ms")
          .defaultValue(60000);

  /**
   * Retryer retry count
   */
  ConfigOption<Integer> MAX_ATTEMPT_COUNT =
      key(WRITER_PREFIX + "max_attempt_count")
          .defaultValue(5);
}
