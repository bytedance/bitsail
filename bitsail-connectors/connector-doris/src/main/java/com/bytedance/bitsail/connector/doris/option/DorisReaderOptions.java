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

package com.bytedance.bitsail.connector.doris.option;

import com.bytedance.bitsail.common.annotation.Essential;
import com.bytedance.bitsail.common.option.ConfigOption;
import com.bytedance.bitsail.common.option.ReaderOptions;

import static com.bytedance.bitsail.common.option.ConfigOptions.key;
import static com.bytedance.bitsail.common.option.ReaderOptions.READER_PREFIX;

public interface DorisReaderOptions extends ReaderOptions.BaseReaderOptions {

  ConfigOption<String> FE_HOSTS =
      key(READER_PREFIX + "fe_hosts")
          .defaultValue("");

  ConfigOption<String> MYSQL_HOSTS =
      key(READER_PREFIX + "mysql_hosts")
          .defaultValue("");

  ConfigOption<String> USER =
      key(READER_PREFIX + "user")
          .defaultValue("root");

  ConfigOption<String> PASSWORD =
      key(READER_PREFIX + "password")
          .defaultValue("");

  @Essential
  ConfigOption<String> DB_NAME =
      key(READER_PREFIX + "db_name")
          .noDefaultValue(String.class);

  @Essential
  ConfigOption<String> TABLE_NAME =
      key(READER_PREFIX + "table_name")
          .noDefaultValue(String.class);

  ConfigOption<Integer> TABLET_SIZE =
      key(READER_PREFIX + "tablet_size")
          .defaultValue(Integer.MAX_VALUE);

  ConfigOption<Long> EXEC_MEM_LIMIT =
      key(READER_PREFIX + "exec_mem_limit")
          .defaultValue(2147483648L);

  ConfigOption<Integer> REQUEST_QUERY_TIMEOUT_S =
      key(READER_PREFIX + "request_query_timeout_s")
          .defaultValue(3600);

  ConfigOption<Integer> REQUEST_BATCH_SIZE =
      key(READER_PREFIX + "request_batch_size")
          .defaultValue(1024);

  ConfigOption<Integer> REQUEST_CONNECT_TIMEOUTS =
      key(READER_PREFIX + "request_connect_timeouts")
          .defaultValue(30 * 1000);

  ConfigOption<Integer> REQUEST_RETRIES =
      key(READER_PREFIX + "request_retries")
          .defaultValue(3);

  ConfigOption<Integer> REQUEST_READ_TIMEOUTS =
      key(READER_PREFIX + "request_read_timeouts")
          .defaultValue(30 * 1000);

  // Options for select data.
  ConfigOption<String> SQL_FILTER =
      key(READER_PREFIX + "sql_filter")
          .noDefaultValue(String.class);

}
