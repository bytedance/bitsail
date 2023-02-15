/*
 * Copyright 2022-2023 Bytedance Ltd. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.bytedance.bitsail.connector.cdc.postgres.source.option;

import com.bytedance.bitsail.common.option.ConfigOption;
import com.bytedance.bitsail.connector.cdc.option.BinlogReaderOptions;

import static com.bytedance.bitsail.common.option.ConfigOptions.key;
import static com.bytedance.bitsail.common.option.ReaderOptions.READER_PREFIX;

public interface PostgresChangeEventOptions extends BinlogReaderOptions {
  ConfigOption<String> PLUGIN_NAME =
          key(READER_PREFIX + "plugin_name")
                  .defaultValue("wal2json");

  ConfigOption<String> SLOT_NAME =
          key(READER_PREFIX + "slot_name")
                  .defaultValue("dts");
}
