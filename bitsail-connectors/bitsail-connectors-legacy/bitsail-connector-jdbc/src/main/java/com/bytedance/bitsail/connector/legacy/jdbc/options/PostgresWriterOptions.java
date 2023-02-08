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

package com.bytedance.bitsail.connector.legacy.jdbc.options;

import com.bytedance.bitsail.common.option.ConfigOption;

import static com.bytedance.bitsail.common.option.ConfigOptions.key;
import static com.bytedance.bitsail.common.option.WriterOptions.WRITER_PREFIX;

/**
 * Created 2022/8/16
 */

public interface PostgresWriterOptions extends JdbcWriterOptions {
  ConfigOption<String> PRIMARY_KEY =
      key(WRITER_PREFIX + "primary_key")
          .noDefaultValue(String.class);

  ConfigOption<String> UPSERT_KEY =
      key(WRITER_PREFIX + "upsert_key")
          .noDefaultValue(String.class);

  ConfigOption<String> TABLE_SCHEMA =
      key(WRITER_PREFIX + "table_schema")
          .defaultValue("public");

  ConfigOption<Boolean> DELETE_THRESHOLD_ENABLED =
      key(WRITER_PREFIX + "delete_threshold_enabled")
          .defaultValue(true);

  ConfigOption<Boolean> IS_TRUNCATE_MODE =
      key(WRITER_PREFIX + "is_truncate_mode")
          .defaultValue(false);
}