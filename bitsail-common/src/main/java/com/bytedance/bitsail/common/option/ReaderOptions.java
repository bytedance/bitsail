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

package com.bytedance.bitsail.common.option;

import com.bytedance.bitsail.common.model.ColumnInfo;

import com.alibaba.fastjson.TypeReference;

import java.util.List;
import java.util.Map;

import static com.bytedance.bitsail.common.option.ConfigOptions.key;

/**
 * The set of configuration options relating to reader config.
 */
public interface ReaderOptions {
  String JOB_READER = "job.reader";
  String READER_PREFIX = JOB_READER + ".";

  ConfigOption<String> READER_CLASS =
      key(READER_PREFIX + "class")
          .noDefaultValue(String.class);

  /**
   * Metric tag for indicating reader type.
   */
  ConfigOption<String> READER_METRIC_TAG_NAME =
      key(READER_PREFIX + "metric_tag_name")
          .noDefaultValue(String.class);

  /**
   * for describing different source operator
   */
  ConfigOption<String> SOURCE_OPERATOR_DESC =
      key(READER_PREFIX + "source_operator.desc")
          .noDefaultValue(String.class);

  /**
   * "job.reader.reader_conf_list": [
   * {
   * "job.reader.connector.type": "kafka",
   * "job.reader.connector.topic": "topic_1",
   * ...
   * },
   * {
   * "job.reader.connector.type": "kafka",
   * "job.reader.connector.topic": "topic_2",
   * ...
   * }
   * ]
   */
  ConfigOption<List<Map<String, Object>>> READER_CONFIG_LIST =
      key(READER_PREFIX + "reader_conf_list")
          .onlyReference(new TypeReference<List<Map<String, Object>>>() {
          });

  interface BaseReaderOptions {

    ConfigOption<List<ColumnInfo>> COLUMNS =
        key(READER_PREFIX + "columns")
            .onlyReference(new TypeReference<List<ColumnInfo>>() {
            });

    ConfigOption<Integer> READER_PARALLELISM_NUM =
        key(READER_PREFIX + "reader_parallelism_num")
            .noDefaultValue(Integer.class);

    ConfigOption<String> CHARSET_NAME =
        key(READER_PREFIX + "charset_name")
            .defaultValue("");

    ConfigOption<String> DB_NAME =
        key(READER_PREFIX + "db_name")
            .noDefaultValue(String.class);

    ConfigOption<String> TABLE_NAME =
        key(READER_PREFIX + "table_name")
            .noDefaultValue(String.class);

    ConfigOption<String> PARTITION =
        key(READER_PREFIX + "partition")
            .noDefaultValue(String.class);

    ConfigOption<String> USER_NAME =
        key(READER_PREFIX + "user_name")
            .noDefaultValue(String.class);

    ConfigOption<String> PASSWORD =
        key(READER_PREFIX + "password")
            .noDefaultValue(String.class);

    ConfigOption<String> CONTENT_TYPE =
        key(READER_PREFIX + "content_type")
            .noDefaultValue(String.class);
  }

  interface BaseFileReaderOptions extends BaseReaderOptions {

    ConfigOption<String> SOURCE_ENGINE =
        key(READER_PREFIX + "source_engine")
            .noDefaultValue(String.class);

    ConfigOption<String> CHARSET_NAME =
        key(READER_PREFIX + "charset_name")
            .defaultValue("");

    ConfigOption<Long> SKIP_LINES =
        key(READER_PREFIX + "skip_lines")
            .defaultValue(0L);

    /**
     * Delimiter used for parse nested column, default is dot
     */
    ConfigOption<String> COLUMN_DELIMITER =
        key(READER_PREFIX + "column_delimiter")
            .defaultValue("\\.");

    ConfigOption<Boolean> CASE_INSENSITIVE =
        key(READER_PREFIX + "case_insensitive")
            .defaultValue(true);

    /**
     * Parse from content offset position (used to filter header lines)
     */
    ConfigOption<Integer> CONTENT_OFFSET =
        key(READER_PREFIX + "content_offset")
            .defaultValue(0);

    ConfigOption<String> BASE64_PROTO_DEFINE =
        key(READER_PREFIX + "base64_proto_define")
            .noDefaultValue(String.class);

    ConfigOption<String> PROTOC_PATH =
        key(READER_PREFIX + "protoc_path")
            .defaultValue("/opt/tiger/protobuf3/bin/protoc");

    ConfigOption<String> TMP_DIR =
        key(READER_PREFIX + "tmp_dir")
            .noDefaultValue(String.class);

    ConfigOption<String> PROTOC_CLASS_NAME =
        key(READER_PREFIX + "protoc_class_name")
            .noDefaultValue(String.class);

    ConfigOption<String> CSV_DELIMITER =
        key(READER_PREFIX + "csv_delimiter")
            .defaultValue(",");

    ConfigOption<Character> CSV_ESCAPE =
        key(READER_PREFIX + "csv_escape")
            .noDefaultValue(Character.class);

    ConfigOption<Character> CSV_QUOTE =
        key(READER_PREFIX + "csv_quote")
            .noDefaultValue(Character.class);

    ConfigOption<String> CSV_WITH_NULL_STRING =
        key(READER_PREFIX + "csv_with_null_string")
            .noDefaultValue(String.class);

    /**
     * A char that will be used as replacement when there are
     * multiple delimiters, default is §
     */
    ConfigOption<Character> CSV_MULTI_DELIMITER_REPLACER =
        key(READER_PREFIX + "csv_multi_delimiter_replace_char")
            .defaultValue('§');

    /**
     * Custom fastjson serialization method, multiple values are separated by commas
     *
     * @see com.alibaba.fastjson.serializer.SerializerFeature
     */
    ConfigOption<String> JSON_SERIALIZER_FEATURES =
        key(READER_PREFIX + "json_serializer_features")
            .noDefaultValue(String.class);

    ConfigOption<String> DESERIALIZER_TYPE =
        key(READER_PREFIX + "deserializer_type")
            .noDefaultValue(String.class);

    ConfigOption<Boolean> ENABLE_COUNT_MODE =
        key(READER_PREFIX + "enable_count_mode")
            .defaultValue(false);
  }

}
