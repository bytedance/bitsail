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

import com.bytedance.bitsail.base.connector.reader.v1.SourceEvent;
import com.bytedance.bitsail.base.connector.reader.v1.SourcePipeline;
import com.bytedance.bitsail.base.connector.reader.v1.SourceReader;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.connector.cdc.source.event.BinlogCompleteAckEvent;
import com.bytedance.bitsail.connector.cdc.source.split.BinlogSplit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;

public abstract class BinlogSourceReader implements SourceReader<Row, BinlogSplit> {

  private static final Logger LOG = LoggerFactory.getLogger(BinlogSourceReader.class);

  protected BitSailConfiguration jobConf;

  protected Context readerContext;

  private final Queue<BinlogSplit> remainSplits;

  protected final BinlogSplitReader<Row> reader;

  private boolean splitSubmitted;

  public BinlogSourceReader(BitSailConfiguration jobConf, SourceReader.Context readerContext) {
    this.jobConf = jobConf;
    this.readerContext = readerContext;
    this.remainSplits = new ArrayDeque<>();
    this.reader = getReader();
    this.splitSubmitted = false;
  }

  public abstract BinlogSplitReader<Row> getReader();

  @Override
  public void start() {
    // do nothing
  }

  @Override
  public void pollNext(SourcePipeline<Row> pipeline) throws Exception {
    // poll from reader
    if (this.reader.isRunning() && this.reader.hasNext()) {
      Row record = this.reader.poll();
      pipeline.output(record);
    }
  }

  @Override
  public void addSplits(List<BinlogSplit> splits) {
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
  public abstract List<BinlogSplit> snapshotState(long checkpointId);

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
      BinlogSplit curSplit = remainSplits.poll();
      LOG.info("submit split to binlog reader: {}, size of the remaining splits: {}", curSplit.toString(), remainSplits.size());
      this.reader.readSplit(curSplit);
    }
  }
}
