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

import com.alibaba.fastjson.TypeReference;

import java.util.List;

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
}
