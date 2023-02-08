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

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.flink.core.io.InputSplit;
import org.apache.hadoop.hbase.mapreduce.TableSplit;

@Data
@AllArgsConstructor
public class RegionSplit implements InputSplit {

  private byte[] startKey;
  private byte[] endKey;
  private byte[] tableName;
  private String regionName;
  private String regionLocation;
  private long length;

  public RegionSplit(TableSplit tableSplit) {
    this.startKey = tableSplit.getStartRow();
    this.endKey = tableSplit.getEndRow();
    this.tableName = tableSplit.getTableName();
    this.regionName = tableSplit.getEncodedRegionName();
    this.length = tableSplit.getLength();
    this.regionLocation = tableSplit.getRegionLocation();
  }

  @Override
  public int getSplitNumber() {
    return 0;
  }
}
