/*
 * Copyright 2022-2023 Bytedance Ltd. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.bytedance.bitsail.connector.cdc.option;

import com.bytedance.bitsail.common.option.ConfigOption;
import com.bytedance.bitsail.common.option.ReaderOptions;
import com.bytedance.bitsail.connector.cdc.model.ClusterInfo;
import com.bytedance.bitsail.connector.cdc.source.offset.BinlogOffsetType;

import com.alibaba.fastjson.TypeReference;

import java.util.List;
import java.util.Map;

import static com.bytedance.bitsail.common.option.ConfigOptions.key;
import static com.bytedance.bitsail.common.option.ReaderOptions.READER_PREFIX;

public interface BinlogReaderOptions extends ReaderOptions.BaseReaderOptions {
  //We should sync with JdbcReaderOptions, to support future migration
  ConfigOption<Boolean> CASE_SENSITIVE =
      key(READER_PREFIX + "case_sensitive")
          .defaultValue(false);

  ConfigOption<List<ClusterInfo>> CONNECTIONS =
      key(READER_PREFIX + "connections")
          .onlyReference(new TypeReference<List<ClusterInfo>>() {
          });

  /**
   * Connection's timeout value when execute query statement.
   */
  ConfigOption<Integer> QUERY_TIMEOUT_SECONDS =
      key(READER_PREFIX + "query_timeout_seconds")
          .defaultValue(300);

  /**
   * Connection's retry number when execute query statement.
   */
  ConfigOption<Integer> QUERY_RETRY_TIMES =
      key(READER_PREFIX + "query_retry_times")
          .defaultValue(3);

  // CDC related options
  ConfigOption<String> INITIAL_OFFSET_TYPE =
      key(READER_PREFIX + "initial_offset_type")
          .defaultValue(String.valueOf(BinlogOffsetType.EARLIEST).toLowerCase());

  ConfigOption<Map<String, String>> INITIAL_OFFSET_PROPS =
      key(READER_PREFIX + "initial_offset_props")
          .onlyReference(new TypeReference<Map<String, String>>() {
          });

  /**
   * Interval to attempt polling message from debezium queue, if there is no record in the queue.
   */
  ConfigOption<Long> POLL_INTERVAL_MS =
      key(READER_PREFIX + "poll_interval_ms")
          .defaultValue(5000L);

  /**
   * Max batch size of debezium message queue.
   */
  ConfigOption<Integer> MAX_BATCH_SIZE =
      key(READER_PREFIX + "max_batch_size")
          .defaultValue(2048);

  ConfigOption<Integer> MAX_QUEUE_SIZE =
      key(READER_PREFIX + "max_queue_size")
          .defaultValue(8192);

  ConfigOption<String> FORMAT_TYPE =
      key(READER_PREFIX + "format_type")
          .defaultValue("debezium_json")
          .withAlias("format");
}
