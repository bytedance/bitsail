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

package com.bytedance.bitsail.connector.cdc.source.split;

import com.bytedance.bitsail.base.connector.reader.v1.SourceSplit;

public class BaseCDCSplit implements SourceSplit {
  protected final String splitId;
  private final Integer splitType;
  public BaseCDCSplit(String splitId, Integer splitType) {
    this.splitId = splitId;
    this.splitType = splitType;
  }

  public boolean isBinlogSplit() {
    return splitType.equals(SplitType.BINLOG_SPLIT_TYPE);
  }

  public boolean isSnapshotSplit() {
    return splitType.equals(SplitType.SNAPSHOT_SPLIT_TYPE);
  }

  @Override
  public String uniqSplitId() {
    return splitId;
  }
}
