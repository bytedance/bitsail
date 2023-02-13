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

package com.bytedance.bitsail.connector.doris.option;

import com.bytedance.bitsail.common.annotation.Essential;
import com.bytedance.bitsail.common.option.ConfigOption;
import com.bytedance.bitsail.common.option.WriterOptions;

import com.alibaba.fastjson.TypeReference;

import java.util.List;
import java.util.Map;

import static com.bytedance.bitsail.common.option.ConfigOptions.key;
import static com.bytedance.bitsail.common.option.WriterOptions.WRITER_PREFIX;

public interface DorisWriterOptions extends WriterOptions.BaseWriterOptions {

  ConfigOption<String> FE_HOSTS =
      key(WRITER_PREFIX + "fe_hosts")
          .defaultValue("");

  ConfigOption<String> MYSQL_HOSTS =
      key(WRITER_PREFIX + "mysql_hosts")
          .defaultValue("");

  ConfigOption<String> USER =
      key(WRITER_PREFIX + "user")
          .defaultValue("root");

  ConfigOption<String> PASSWORD =
      key(WRITER_PREFIX + "password")
          .defaultValue("");

  @Essential
  ConfigOption<String> DB_NAME =
      key(WRITER_PREFIX + "db_name")
          .noDefaultValue(String.class);

  @Essential
  ConfigOption<String> TABLE_NAME =
      key(WRITER_PREFIX + "table_name")
          .noDefaultValue(String.class);

  ConfigOption<Boolean> TABLE_HAS_PARTITION =
      key(WRITER_PREFIX + "table_has_partition")
          .defaultValue(true);

  ConfigOption<List<Map<String, Object>>> PARTITIONS =
      key(WRITER_PREFIX + "partitions")
          .onlyReference(new TypeReference<List<Map<String, Object>>>() {
          });

  ConfigOption<Integer> SINK_FLUSH_INTERVAL_MS =
      key(WRITER_PREFIX + "sink_flush_interval_ms")
          .defaultValue(5000);

  ConfigOption<Integer> SINK_MAX_RETRIES =
      key(WRITER_PREFIX + "sink_max_retries")
          .defaultValue(3);

  ConfigOption<Integer> SINK_RECORD_SIZE =
      key(WRITER_PREFIX + "sink_record_size")
          .defaultValue(20 * 1024 * 1024);

  ConfigOption<Integer> SINK_RECORD_COUNT =
      key(WRITER_PREFIX + "sink_record_count")
          .defaultValue(100000);

  ConfigOption<Integer> SINK_BUFFER_SIZE =
      key(WRITER_PREFIX + "sink_buffer_size")
          .defaultValue(1024 * 1024);

  ConfigOption<Integer> SINK_BUFFER_COUNT =
      key(WRITER_PREFIX + "sink_buffer_count")
          .defaultValue(3);

  ConfigOption<String> SINK_LABEL_PREFIX =
      key(WRITER_PREFIX + "sink_label_prefix")
          .defaultValue("");

  ConfigOption<Boolean> SINK_ENABLE_DELETE =
      key(WRITER_PREFIX + "sink_enable_delete")
          .defaultValue(false);

  ConfigOption<Boolean> SINK_ENABLE_2PC =
      key(WRITER_PREFIX + "sink_enable_2PC")
          .defaultValue(true);

  ConfigOption<Integer> REQUEST_CONNECT_TIMEOUTS =
      key(WRITER_PREFIX + "request_connect_timeouts")
          .defaultValue(30 * 1000);

  ConfigOption<Integer> REQUEST_READ_TIMEOUTS =
      key(WRITER_PREFIX + "request_read_timeouts")
          .defaultValue(30 * 1000);

  ConfigOption<Integer> REQUEST_RETRIES =
      key(WRITER_PREFIX + "request_retries")
          .defaultValue(3);

  ConfigOption<String> SINK_WRITE_MODE =
      key(WRITER_PREFIX + "sink_write_mode")
          .defaultValue("BATCH_UPSERT");

  ConfigOption<Map<String, String>> STREAM_LOAD_PROPERTIES =
      key(WRITER_PREFIX + "stream_load_properties")
          .onlyReference(new TypeReference<Map<String, String>>() {
          });

  ConfigOption<String> LOAD_CONTEND_TYPE =
      key(WRITER_PREFIX + "load_contend_type")
          .defaultValue("json");

  ConfigOption<String> CSV_FIELD_DELIMITER =
      key(WRITER_PREFIX + "csv_field_delimiter")
          .defaultValue(",");

  ConfigOption<String> CSV_LINE_DELIMITER =
      key(WRITER_PREFIX + "csv_line_delimiter")
          .defaultValue("\n");
}
