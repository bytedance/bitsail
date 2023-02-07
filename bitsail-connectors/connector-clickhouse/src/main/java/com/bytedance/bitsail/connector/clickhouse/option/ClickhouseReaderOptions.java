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

package com.bytedance.bitsail.connector.clickhouse.option;

import com.bytedance.bitsail.common.annotation.Essential;
import com.bytedance.bitsail.common.option.ConfigOption;
import com.bytedance.bitsail.common.option.ReaderOptions;

import com.alibaba.fastjson.TypeReference;

import java.util.Map;

import static com.bytedance.bitsail.common.option.ConfigOptions.key;
import static com.bytedance.bitsail.common.option.ReaderOptions.READER_PREFIX;

public interface ClickhouseReaderOptions extends ReaderOptions.BaseReaderOptions {
  /**
   * Standard format:
   * jdbc:(ch|clickhouse)[:protocol]://endpoint[,endpoint][/database][?parameters][#tags]<br/>
   *  - endpoint: [protocol://]host[:port][/database][?parameters][#tags]<br/>
   *  - protocol: (grpc|grpcs|http|https|tcp|tcps)
   */
  @Essential
  ConfigOption<String> JDBC_URL =
      key(READER_PREFIX + "jdbc_url")
          .noDefaultValue(String.class);

  ConfigOption<String> SPLIT_FIELD =
      key(READER_PREFIX + "split_field")
          .noDefaultValue(String.class);

  ConfigOption<String> SPLIT_CONFIGURATION =
      key(READER_PREFIX + "split_config")
          .noDefaultValue(String.class);

  // Options for select data.
  ConfigOption<String> SQL_FILTER =
      key(READER_PREFIX + "sql_filter")
          .noDefaultValue(String.class);

  ConfigOption<Long> MAX_FETCH_COUNT =
      key(READER_PREFIX + "max_fetch_count")
          .noDefaultValue(Long.class);

  // Connection properties.
  ConfigOption<Map<String, String>> CUSTOMIZED_CONNECTION_PROPERTIES =
      key(READER_PREFIX + "customized_connection_properties")
          .onlyReference(new TypeReference<Map<String, String>>() {});
}
