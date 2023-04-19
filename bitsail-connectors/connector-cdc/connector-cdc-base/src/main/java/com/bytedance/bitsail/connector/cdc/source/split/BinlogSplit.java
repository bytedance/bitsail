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

import com.bytedance.bitsail.connector.cdc.source.offset.BinlogOffset;

import java.util.HashMap;
import java.util.Map;

public class BinlogSplit extends BaseCDCSplit {
  private static final long serialVersionUID = 2L;

  private final BinlogOffset beginOffset;

  private final BinlogOffset endOffset;

  //Store schema of each table
  private final Map<String, String> schemas;

  //TODO: optimize serialization for each checkpoint
  transient byte[] cache;

  public BinlogSplit(String splitId, BinlogOffset begin, BinlogOffset end) {
    this(splitId, begin, end, new HashMap<>());
  }

  public BinlogSplit(String splitId, BinlogOffset begin, BinlogOffset end, Map<String, String> schemas) {
    super(splitId, SplitType.BINLOG_SPLIT_TYPE);
    this.beginOffset = begin;
    this.endOffset = end;
    this.schemas = schemas;
  }

  public BinlogOffset getBeginOffset() {
    return beginOffset;
  }

  public BinlogOffset getEndOffset() {
    return endOffset;
  }

  public Map<String, String> getSchemas() {
    return schemas;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    this.schemas.forEach((k, v) -> sb.append(k).append(":").append(v).append(","));
    return String.format("Binlog split ID: %s, begin offset: %s, end offset: %s, schema: %s",
        this.splitId,
        this.beginOffset.toString(),
        this.endOffset.toString(), sb);
  }
}
