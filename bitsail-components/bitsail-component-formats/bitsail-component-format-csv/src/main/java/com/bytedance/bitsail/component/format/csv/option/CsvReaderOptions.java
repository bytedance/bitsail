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

package com.bytedance.bitsail.component.format.csv.option;

import com.bytedance.bitsail.common.option.ConfigOption;
import com.bytedance.bitsail.common.option.ReaderOptions;

import static com.bytedance.bitsail.common.option.ConfigOptions.key;
import static com.bytedance.bitsail.common.option.ReaderOptions.READER_PREFIX;

/**
 * Ftp Options
 */
public interface CsvReaderOptions extends ReaderOptions.BaseReaderOptions {

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
}
