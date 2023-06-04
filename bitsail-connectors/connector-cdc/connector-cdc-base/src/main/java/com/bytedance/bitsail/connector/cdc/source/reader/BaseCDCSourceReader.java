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

package com.bytedance.bitsail.connector.cdc.source.reader;

import com.bytedance.bitsail.base.connector.reader.v1.Boundedness;
import com.bytedance.bitsail.base.connector.reader.v1.SourceEvent;
import com.bytedance.bitsail.base.connector.reader.v1.SourcePipeline;
import com.bytedance.bitsail.base.connector.reader.v1.SourceReader;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.component.format.debezium.deserialization.DebeziumDeserializationSchema;
import com.bytedance.bitsail.connector.cdc.source.event.BinlogCompleteAckEvent;
import com.bytedance.bitsail.connector.cdc.source.split.BaseCDCSplit;
import com.bytedance.bitsail.connector.cdc.source.split.BinlogSplit;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;

public abstract class BaseCDCSourceReader implements SourceReader<Row, BaseCDCSplit> {

  private static final Logger LOG = LoggerFactory.getLogger(BaseCDCSourceReader.class);

  private final Queue<BaseCDCSplit> remainSplits;

  private boolean splitSubmitted;

  protected final BitSailConfiguration readerConf;

  protected final BitSailConfiguration commonConf;

  protected final Context readerContext;

  protected final DebeziumDeserializationSchema deserializationSchema;

  protected final BinlogSplitReader<SourceRecord> reader;

  private final Boundedness boundedness;

  public BaseCDCSourceReader(BitSailConfiguration readerConf,
                             BitSailConfiguration commonConf,
                             SourceReader.Context readerContext,
                             DebeziumDeserializationSchema deserializationSchema,
                             Boundedness boundedness) {
    this.readerConf = readerConf;
    this.commonConf = commonConf;
    this.readerContext = readerContext;
    this.remainSplits = new ArrayDeque<>();
    this.reader = getReader();
    this.splitSubmitted = false;
    this.deserializationSchema = deserializationSchema;
    this.boundedness = boundedness;

    this.deserializationSchema.open();
  }

  public abstract BinlogSplitReader<SourceRecord> getReader();

  @Override
  public void start() {
    // do nothing
  }

  @Override
  public void pollNext(SourcePipeline<Row> pipeline) throws Exception {
    // poll from reader
    if (reader.isRunning() && reader.hasNext()) {
      SourceRecord sourceRecord = reader.poll();
      deserializationSchema.deserialize(sourceRecord, pipeline);
    }
  }

  @Override
  public void addSplits(List<BaseCDCSplit> splits) {
    LOG.info("Received splits from coordinator.");
    splits.forEach(e -> LOG.info("Add split: {}", e.toString()));
    remainSplits.addAll(splits);
    if (!splitSubmitted) {
      submitSplit();
      splitSubmitted = true;
    }
  }

  @Override
  public boolean hasMoreElements() {
    if (Boundedness.UNBOUNDEDNESS.equals(boundedness)) {
      return true;
    }
    if (!splitSubmitted) {
      return true;
    } else {
      return this.reader.hasNext();
    }
  }

  @Override
  public void notifyNoMoreSplits() {
    // do nothing
  }

  @Override
  public void handleSourceEvent(SourceEvent sourceEvent) {
    if (sourceEvent instanceof BinlogCompleteAckEvent) {
      LOG.info("Binlog completed acked by coordinator.");
    }
  }

  /**
   * Snapshot the offset into state.
   */
  @Override
  public abstract List<BaseCDCSplit> snapshotState(long checkpointId);

  @Override
  public void notifyCheckpointComplete(long checkpointId) throws Exception {
    // do nothing
  }

  @Override
  public void close() {
    if (this.reader != null && this.reader.isRunning()) {
      this.reader.close();
      LOG.info("Reader close successfully");
    }
  }

  private void submitSplit() {
    if (!remainSplits.isEmpty()) {
      BinlogSplit curSplit = (BinlogSplit) remainSplits.poll();
      LOG.info("submit split to binlog reader: {}, size of the remaining splits: {}", curSplit.toString(), remainSplits.size());
      this.reader.readSplit(curSplit);
    }
  }
}
