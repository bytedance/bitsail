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

package com.bytedance.bitsail.connector.oss.option;

import com.bytedance.bitsail.common.option.ConfigOption;
import com.bytedance.bitsail.common.option.ReaderOptions;

import static com.bytedance.bitsail.common.option.ConfigOptions.key;
import static com.bytedance.bitsail.common.option.ReaderOptions.READER_PREFIX;

public interface OssReaderOptions extends ReaderOptions.BaseReaderOptions {

  public static final ConfigOption<String> ACCESS_KEY =
      key(READER_PREFIX + "access_key")
          .noDefaultValue(String.class);

  public static final ConfigOption<String> ACCESS_SECRET =
      key(READER_PREFIX + "access_secret")
          .noDefaultValue(String.class);

  public static final ConfigOption<String> ENDPOINT =
      key(READER_PREFIX + "endpoint")
          .noDefaultValue(String.class);

  public static final ConfigOption<String> BUCKET =
      key(READER_PREFIX + "bucket")
          .noDefaultValue(String.class);

  ConfigOption<String> FILE_PATH =
      key(READER_PREFIX + "file_path")
          .noDefaultValue(String.class);

  ConfigOption<String> CONTENT_TYPE =
      key(READER_PREFIX + "content_type")
          .defaultValue("csv");

  /**
   * CSV Format Options
   */
  // whether to treat error column as null when parsing
  ConfigOption<Boolean> CONVERT_ERROR_COLUMN_AS_NULL =
      key(READER_PREFIX + "convert_error_column_as_null")
          .defaultValue(false);

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

  ConfigOption<Character> CSV_MULTI_DELIMITER_REPLACER =
      key(READER_PREFIX + "csv_multi_delimiter_replace_char")
          .defaultValue('§');

  ConfigOption<Boolean> SKIP_FIRST_LINE =
      key(READER_PREFIX + "skip_first_line")
          .defaultValue(false);

  /**
   * JSON Options
   * <p>
   * Tips:
   * CONVERT_ERROR_COLUMN_AS_NULL is set above
   */
  // whether to be insensitive to upper or lower case
  ConfigOption<Boolean> CASE_INSENSITIVE =
      key(READER_PREFIX + "case_insensitive")
          .defaultValue(false);
}
