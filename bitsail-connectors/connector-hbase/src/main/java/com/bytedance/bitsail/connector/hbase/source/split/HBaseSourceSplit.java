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

package com.bytedance.bitsail.connector.hbase.source.split;

import com.bytedance.bitsail.base.connector.reader.v1.SourceSplit;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class HBaseSourceSplit implements SourceSplit {
  public static final String HBASE_SOURCE_SPLIT_PREFIX = "hbase_source_split_";
  private final String splitId;
  private byte[] startRow;
  private byte[] endRow;

  /**
   * Read whole table or not
   */
  private boolean readTable;

  public HBaseSourceSplit(int splitId) {
    this.splitId = HBASE_SOURCE_SPLIT_PREFIX + splitId;
  }

  public HBaseSourceSplit(int splitId, byte[] startRow, byte[] endRow) {
    this.splitId = HBASE_SOURCE_SPLIT_PREFIX + splitId;
    this.startRow = startRow;
    this.endRow = endRow;
  }

  @Override
  public String uniqSplitId() {
    return splitId;
  }

  @Override
  public boolean equals(Object obj) {
    return (obj instanceof HBaseSourceSplit) && (splitId.equals(((HBaseSourceSplit) obj).splitId));
  }

  @Override
  public String toString() {
    return String.format(
        "{\"split_id\":\"%s\", \"readTable\":%s}",
        splitId, readTable);
  }
}
