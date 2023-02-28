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

package com.bytedance.bitsail.test.e2e.option;

import com.bytedance.bitsail.common.option.ConfigOption;

import static com.bytedance.bitsail.common.option.CommonOptions.COMMON_PREFIX;
import static com.bytedance.bitsail.common.option.ConfigOptions.key;
import static com.bytedance.bitsail.common.option.ReaderOptions.READER_PREFIX;
import static com.bytedance.bitsail.common.option.WriterOptions.WRITER_PREFIX;

public interface EndToEndOptions {

  ConfigOption<String> E2E_READER_DATA_SOURCE_CLASS =
      key(READER_PREFIX + "e2e_data_source_class")
          .noDefaultValue(String.class);

  ConfigOption<String> E2E_WRITER_DATA_SOURCE_CLASS =
      key(WRITER_PREFIX + "e2e_data_source_class")
          .noDefaultValue(String.class);

  ConfigOption<String> E2E_GENERIC_EXECUTOR_SETTING_FILE =
      key(COMMON_PREFIX + "e2e_generic_executor_setting_file")
          .noDefaultValue(String.class);
}
