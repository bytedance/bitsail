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

package com.bytedance.bitsail.connector.cdc.sqlserver.source.reader;

import com.bytedance.bitsail.base.connector.reader.v1.Boundedness;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.option.CommonOptions;
import com.bytedance.bitsail.component.format.debezium.deserialization.DebeziumDeserializationSchema;
import com.bytedance.bitsail.connector.cdc.source.offset.BinlogOffset;
import com.bytedance.bitsail.connector.cdc.source.reader.BaseCDCSourceReader;
import com.bytedance.bitsail.connector.cdc.source.reader.BinlogSplitReader;
import com.bytedance.bitsail.connector.cdc.source.split.BaseCDCSplit;
import com.bytedance.bitsail.connector.cdc.source.split.BinlogSplit;
import com.bytedance.bitsail.connector.cdc.sqlserver.source.util.DebeziumUtils;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SqlServerCDCSourceReader extends BaseCDCSourceReader {
  private static final Logger LOG = LoggerFactory.getLogger(SqlServerCDCSourceReader.class);

  public SqlServerCDCSourceReader(BitSailConfiguration readerConf,
                                  BitSailConfiguration commonConf,
                                  Context readerContext,
                                  DebeziumDeserializationSchema deserializationSchema,
                                  Boundedness boundedness) {
    super(readerConf, commonConf, readerContext, deserializationSchema, boundedness);
  }

  @Override
  public List<BaseCDCSplit> snapshotState(long checkpointId) {
    LOG.info("SnapshotState on SqlServerCDCSourceReader with checkpoint ID: " + checkpointId);
    // store the latest offset
    Map<String, String> readerOffset = this.reader.getOffset();
    BinlogOffset offset = DebeziumUtils.convertDbzOffsetToBinlogOffset(readerOffset);
    List<BaseCDCSplit> splits = new ArrayList<>();
    //TODO: Store the schema each checkpoint
    BinlogSplit split = new BinlogSplit("sqlserver-binlog-0", offset, BinlogOffset.boundless());
    splits.add(split);
    LOG.info("Snapshot binlog split: " + split);
    return splits;
  }

  @Override
  public BinlogSplitReader<SourceRecord> getReader() {
    return new SqlServerBinlogSplitReader(readerConf,
        readerContext.getIndexOfSubtask(),
        commonConf.get(CommonOptions.INSTANCE_ID));
  }
}
