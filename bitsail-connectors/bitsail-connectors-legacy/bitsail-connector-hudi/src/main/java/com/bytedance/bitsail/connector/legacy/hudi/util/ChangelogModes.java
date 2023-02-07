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

package com.bytedance.bitsail.connector.legacy.hudi.util;

import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.types.RowKind;

/**
 * Utilities for all kinds of common {@link ChangelogMode}s.
 */
public class ChangelogModes {
  public static final ChangelogMode FULL = ChangelogMode.newBuilder()
      .addContainedKind(RowKind.INSERT)
      .addContainedKind(RowKind.UPDATE_BEFORE)
      .addContainedKind(RowKind.UPDATE_AFTER)
      .addContainedKind(RowKind.DELETE)
      .build();

  /**
   * Change log mode that ignores UPDATE_BEFORE, e.g UPSERT.
   */
  public static final ChangelogMode UPSERT = ChangelogMode.newBuilder()
      .addContainedKind(RowKind.INSERT)
      .addContainedKind(RowKind.UPDATE_AFTER)
      .addContainedKind(RowKind.DELETE)
      .build();

  private ChangelogModes() {
  }
}
