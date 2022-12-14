/*
 * Copyright 2022-present, Bytedance Ltd.
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

package com.bytedance.bitsail.connector.legacy.hudi.sink.transform;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hudi.common.model.HoodieRecord;

/**
 * Utilities for {@link RowDataToHoodieFunction}.
 */
public abstract class RowDataToHoodieFunctions {
  private RowDataToHoodieFunctions() {
  }

  /**
   * Creates a {@link RowDataToHoodieFunction} instance based on the given configuration.
   */
  @SuppressWarnings("rawtypes")
  public static RowDataToHoodieFunction<RowData, HoodieRecord> create(RowType rowType, Configuration conf) {
    return new RowDataToHoodieFunction<>(rowType, conf);
  }
}
