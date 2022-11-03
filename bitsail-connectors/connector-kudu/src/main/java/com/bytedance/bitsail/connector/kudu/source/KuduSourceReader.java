/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bytedance.bitsail.connector.kudu.source;

import com.bytedance.bitsail.base.connector.reader.v1.SourcePipeline;
import com.bytedance.bitsail.base.connector.reader.v1.SourceReader;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.model.ColumnInfo;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.connector.kudu.core.KuduFactory;
import com.bytedance.bitsail.connector.kudu.error.KuduErrorCode;
import com.bytedance.bitsail.connector.kudu.option.KuduReaderOptions;
import com.bytedance.bitsail.connector.kudu.source.split.KuduSourceSplit;

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

  private final List<ColumnInfo> columns;
  private final String tableName;

  private final KuduFactory kuduFactory;
  private final KuduScannerConstructor scannerConstructor;

  private Deque<KuduSourceSplit> splits;
  private transient KuduRowDeserializer rowDeserializer;

  public KuduSourceReader(BitSailConfiguration jobConf) {
    this.columns = jobConf.getNecessaryOption(KuduReaderOptions.COLUMNS, KuduErrorCode.REQUIRED_VALUE);
    this.tableName = jobConf.getNecessaryOption(KuduReaderOptions.KUDU_TABLE_NAME, KuduErrorCode.REQUIRED_VALUE);

    this.kuduFactory = new KuduFactory(jobConf, "reader");
    this.scannerConstructor = new KuduScannerConstructor(jobConf);
    this.rowDeserializer = new KuduRowDeserializer(jobConf);

    LOG.info("KuduReader is initialized.");
  }

  @Override
  public void start() {}

  @Override
  public void pollNext(SourcePipeline<Row> pipeline) throws Exception {
    KuduSourceSplit split = splits.poll();
    LOG.info("Begin to read split: {}", split.uniqSplitId());

    KuduScanner scanner = scannerConstructor.createScanner(kuduFactory.getClient(), tableName, split);
    while (scanner.hasMoreRows()) {
      RowResultIterator rowResults = scanner.nextRows();
      while (rowResults.hasNext()) {
        RowResult rowResult = rowResults.next();
        Row row = rowDeserializer.convert(rowResult);
        pipeline.output(row);
      }
    }
    scanner.close();

    LOG.info("Finish reading split: {}", split.uniqSplitId());
  }

  @Override
  public void addSplits(List<KuduSourceSplit> splitList) {
    if (splits == null) {
      this.splits = new LinkedList<>();
    }
    splits.addAll(splitList);
  }

  @Override
  public boolean hasMoreElements() {
    return !splits.isEmpty();
  }

  @Override
  public List<KuduSourceSplit> snapshotState(long checkpointId) {
    return Collections.emptyList();
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) throws Exception {
    //Ignore
  }

  @Override
  public void close() throws Exception {
    kuduFactory.close();
  }
}
