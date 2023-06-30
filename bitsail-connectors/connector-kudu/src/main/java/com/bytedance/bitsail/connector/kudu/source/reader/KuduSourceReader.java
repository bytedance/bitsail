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

package com.bytedance.bitsail.connector.kudu.source.reader;

import com.bytedance.bitsail.base.connector.reader.v1.SourcePipeline;
import com.bytedance.bitsail.base.connector.reader.v1.SourceReader;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.connector.kudu.core.KuduFactory;
import com.bytedance.bitsail.connector.kudu.error.KuduErrorCode;
import com.bytedance.bitsail.connector.kudu.option.KuduReaderOptions;
import com.bytedance.bitsail.connector.kudu.source.split.KuduSourceSplit;

import org.apache.kudu.client.KuduScanToken;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.RowResult;
import org.apache.kudu.client.RowResultIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;

public class KuduSourceReader implements SourceReader<Row, KuduSourceSplit> {
  private static final Logger LOG = LoggerFactory.getLogger(KuduSourceReader.class);

  private final int subTaskId;

  private final String tableName;

  private final KuduFactory kuduFactory;
  private int totalSplitNum = 0;
  private boolean hasNoMoreSplits = false;
  private final Deque<KuduSourceSplit> splits;
  private final transient KuduRowDeserializer rowDeserializer;
  private KuduSourceSplit currentSplit;
  private KuduScanner currentScanner;
  private long currentScanCount;

  public KuduSourceReader(BitSailConfiguration jobConf, SourceReader.Context readerContext) {
    this.subTaskId = readerContext.getIndexOfSubtask();
    this.tableName = jobConf.getNecessaryOption(KuduReaderOptions.KUDU_TABLE_NAME, KuduErrorCode.REQUIRED_VALUE);

    this.kuduFactory = KuduFactory.initReaderFactory(jobConf);
    this.rowDeserializer = new KuduRowDeserializer(readerContext.getRowTypeInfo());
    this.splits = new LinkedList<>();
    LOG.info("KuduReader is initialized.");
  }

  @Override
  public void start() {}

  @Override
  public void pollNext(SourcePipeline<Row> pipeline) throws Exception {
    if (currentScanner == null && splits.isEmpty()) {
      return;
    }

    if (currentScanner == null) {
      this.currentSplit = splits.poll();
      LOG.info("Task {} begins to read split: {}=[{}]", subTaskId, currentSplit.uniqSplitId(), currentSplit.toFormatString(kuduFactory.getSchema(tableName)));
      this.currentScanner = KuduScanToken.deserializeIntoScanner(currentSplit.getSerializedScanToken(), kuduFactory.getClient());
      this.currentScanCount = 0;
    }

    if (currentScanner.hasMoreRows()) {
      RowResultIterator rowResults = currentScanner.nextRows();
      while (rowResults.hasNext()) {
        RowResult rowResult = rowResults.next();
        Row row = rowDeserializer.convert(rowResult);
        pipeline.output(row);
      }
      currentScanCount += rowResults.getNumRows();
    } else {
      currentScanner.close();
      currentScanner = null;
      LOG.info("Task {} finishes reading {} rows from split: {}", subTaskId, currentScanCount, currentSplit.uniqSplitId());
    }
  }

  @Override
  public void addSplits(List<KuduSourceSplit> splitList) {
    totalSplitNum += splitList.size();
    splits.addAll(splitList);
  }

  @Override
  public void notifyNoMoreSplits() {
    this.hasNoMoreSplits = true;
    LOG.info("No more splits will be assigned.");
  }

  @Override
  public boolean hasMoreElements() {
    if (hasNoMoreSplits && splits.isEmpty() && currentScanner == null) {
      LOG.info("Finish reading all {} splits.", totalSplitNum);
      return false;
    }
    return true;
  }

  @Override
  public List<KuduSourceSplit> snapshotState(long checkpointId) {
    return Collections.emptyList();
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) {
    //Ignore
  }

  @Override
  public void close() throws Exception {
    kuduFactory.close();
  }
}
