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
package com.bytedance.bitsail.connector.cdc.postgres.source.reader;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.connector.cdc.postgres.source.constant.PostgresConstant;
import com.bytedance.bitsail.connector.cdc.source.offset.BinlogOffset;
import com.bytedance.bitsail.connector.cdc.source.reader.BinlogSourceReader;
import com.bytedance.bitsail.connector.cdc.source.reader.BinlogSplitReader;
import com.bytedance.bitsail.connector.cdc.source.split.BinlogSplit;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PostgresChangeEventReader extends BinlogSourceReader {
  public PostgresChangeEventReader(BitSailConfiguration jobConf, Context readerContext) {
    super(jobConf, readerContext);
  }

  @Override
  public BinlogSplitReader<Row> getReader() {
    return new PostgresChangeEventSplitReader(jobConf, readerContext.getIndexOfSubtask());
  }

  @Override
  public List<BinlogSplit> snapshotState(long checkpointId) {
    // store the latest offset
    Map<String, String> readerOffset = this.reader.getOffset();
    BinlogOffset offset = BinlogOffset.specified();
    offset.addProps(PostgresConstant.LSN, readerOffset.get(PostgresConstant.LSN));
    offset.addProps(PostgresConstant.TS_USEC, readerOffset.get(PostgresConstant.TS_USEC));
    List<BinlogSplit> splits = new ArrayList<>();
    BinlogSplit split = BinlogSplit.builder()
            .splitId("binlog-0")
            .beginOffset(offset)
            .endOffset(BinlogOffset.boundless())
            .build();
    splits.add(split);
    return splits;
  }
}
