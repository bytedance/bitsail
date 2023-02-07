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

package com.bytedance.bitsail.connector.ftp.option;

import com.bytedance.bitsail.common.option.ConfigOption;
import com.bytedance.bitsail.common.option.ReaderOptions;

import static com.bytedance.bitsail.common.option.CommonOptions.COMMON_PREFIX;
import static com.bytedance.bitsail.common.option.ConfigOptions.key;
import static com.bytedance.bitsail.common.option.ReaderOptions.READER_PREFIX;

/**
 * Ftp Options
 */
public interface FtpReaderOptions extends ReaderOptions.BaseReaderOptions {
  ConfigOption<Integer> PORT =
            key(READER_PREFIX + "port")
                    .defaultValue(21);

  ConfigOption<String> HOST =
            key(READER_PREFIX + "host")
                    .noDefaultValue(String.class);

  ConfigOption<String> PATH_LIST =
            key(READER_PREFIX + "path_list")
                    .noDefaultValue(String.class);

  ConfigOption<String> USER =
            key(READER_PREFIX + "user")
                    .noDefaultValue(String.class);

  ConfigOption<String> PASSWORD =
            key(READER_PREFIX + "password")
                    .noDefaultValue(String.class);

  ConfigOption<Boolean> SKIP_FIRST_LINE =
            key(READER_PREFIX + "skip_first_line")
                    .defaultValue(false);

  ConfigOption<Boolean> ENABLE_SUCCESS_FILE_CHECK =
            key(READER_PREFIX + "enable_success_file_check")
                    .defaultValue(true);

  ConfigOption<String> SUCCESS_FILE_PATH =
            key(READER_PREFIX + "success_file_path")
                    .noDefaultValue(String.class);

  // client connection timeout in milliseconds
  ConfigOption<Integer> TIME_OUT =
            key(READER_PREFIX + "time_out")
                    .defaultValue(5000);

  // max success file retry time, default 60
  ConfigOption<Integer> MAX_RETRY_TIME =
            key(READER_PREFIX + "max_retry_time")
                    .defaultValue(60);

  // interval between retries, default 60s
  ConfigOption<Integer> RETRY_INTERVAL_MS =
            key(READER_PREFIX + "retry_interval_ms")
                    .defaultValue(60000);

  // protocol, FTP or SFTP
  ConfigOption<String> PROTOCOL =
            key(READER_PREFIX + "protocol")
                    .defaultValue("FTP");

  // In ftp mode, connect pattern can be PASV or PORT
  // In sftp mode, connect pattern is NULL
  ConfigOption<String> CONNECT_PATTERN =
            key(READER_PREFIX + "connect_pattern")
                    .defaultValue("PASV");

  ConfigOption<Boolean> CASE_INSENSITIVE =
            key(COMMON_PREFIX + "case_insensitive")
                    .defaultValue(true);

  ConfigOption<String> JSON_SERIALIZER_FEATURES =
      key(COMMON_PREFIX + "json_serializer_features")
          .noDefaultValue(String.class);

  ConfigOption<Boolean> CONVERT_ERROR_COLUMN_AS_NULL =
      key(COMMON_PREFIX + "convert_error_column_as_null")
          .defaultValue(false);

  ConfigOption<String> CSV_DELIMITER =
            key(COMMON_PREFIX + "csv_delimiter")
                    .defaultValue(",");

  ConfigOption<Character> CSV_ESCAPE =
            key(COMMON_PREFIX + "csv_escape")
                    .noDefaultValue(Character.class);

  ConfigOption<Character> CSV_QUOTE =
            key(COMMON_PREFIX + "csv_quote")
                    .noDefaultValue(Character.class);

  ConfigOption<String> CSV_WITH_NULL_STRING =
            key(COMMON_PREFIX + "csv_with_null_string")
                    .noDefaultValue(String.class);

  ConfigOption<Character> CSV_MULTI_DELIMITER_REPLACER =
            key(COMMON_PREFIX + "csv_multi_delimiter_replace_char")
                    .defaultValue('§');
}
