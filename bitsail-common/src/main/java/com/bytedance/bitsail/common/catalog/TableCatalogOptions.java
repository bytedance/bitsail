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

package com.bytedance.bitsail.common.catalog;

import com.bytedance.bitsail.common.option.CommonOptions;
import com.bytedance.bitsail.common.option.ConfigOption;

import com.google.common.annotations.Beta;

import static com.bytedance.bitsail.common.option.ConfigOptions.key;

public interface TableCatalogOptions extends CommonOptions {

  ConfigOption<String> COLUMN_ALIGN_STRATEGY =
      key(COMMON_PREFIX + "column_align_strategy")
          .defaultValue("disable");

  /**
   * Whether enable the ddl sync feature.
   */
  ConfigOption<Boolean> SYNC_DDL =
      key(COMMON_PREFIX + "sync_ddl")
          .defaultValue(false);

  ConfigOption<Boolean> SYNC_DDL_SKIP_ERROR_COLUMNS =
      key(COMMON_PREFIX + "sync_ddl_skip_error_columns")
          .defaultValue(true);

  ConfigOption<Boolean> SYNC_DDL_PRE_EXECUTE =
      key(COMMON_PREFIX + "sync_ddl_pre_execute")
          .defaultValue(false);

  /**
   * Ignore ddl delete fields.
   */
  ConfigOption<Boolean> SYNC_DDL_IGNORE_DROP =
      key(COMMON_PREFIX + "sync_ddl_ignore_drop")
          .defaultValue(true);
  /**
   * Ignore ddl new added fields.
   */
  ConfigOption<Boolean> SYNC_DDL_IGNORE_ADD =
      key(COMMON_PREFIX + "sync_ddl_ignore_add")
          .defaultValue(false);

  /**
   * Ignore ddl updated fields.
   */
  ConfigOption<Boolean> SYNC_DDL_IGNORE_UPDATE =
      key(COMMON_PREFIX + "sync_ddl_ignore_update")
          .defaultValue(false);

  /**
   * Only test in develop
   */
  @Beta
  ConfigOption<Boolean> SYNC_DDL_CREATE_TABLE =
      key(COMMON_PREFIX + "sync_ddl_create_table")
          .defaultValue(false);

  ConfigOption<Boolean> SYNC_DDL_CASE_INSENSITIVE =
      key(COMMON_PREFIX + "sync_ddl_case_insensitive")
          .defaultValue(true);
}
