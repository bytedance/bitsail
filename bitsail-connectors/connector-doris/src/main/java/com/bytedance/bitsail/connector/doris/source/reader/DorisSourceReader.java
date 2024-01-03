/*
 * Copyright 2022 Bytedance Ltd. and/or its affiliates.
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

package com.bytedance.bitsail.connector.doris.source.reader;

import com.bytedance.bitsail.base.connector.reader.v1.SourcePipeline;
import com.bytedance.bitsail.base.connector.reader.v1.SourceReader;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.connector.doris.config.DorisExecutionOptions;
import com.bytedance.bitsail.connector.doris.config.DorisOptions;
import com.bytedance.bitsail.connector.doris.source.split.DorisSourceSplit;
import com.bytedance.bitsail.connector.doris.source.split.DorisSourceSplitReader;

import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;

/**
 * A {@link SourceReader} that read records from {@link DorisSourceSplit}.
 **/
public class DorisSourceReader implements SourceReader<Row, DorisSourceSplit> {
  private static final Logger LOG = LoggerFactory.getLogger(DorisSourceReader.class);
  private final transient Set<DorisSourceSplit> assignedDorisSplits;
  private final transient Set<DorisSourceSplit> finishedDorisSplits;
  private final transient Context context;
  private boolean noMoreSplits;
  private final DorisExecutionOptions executionOptions;
  private final DorisOptions dorisOptions;

  public DorisSourceReader(SourceReader.Context readerContext,
      DorisExecutionOptions executionOptions, DorisOptions dorisOptions) {
    context = readerContext;
    this.assignedDorisSplits = Sets.newHashSet();
    this.finishedDorisSplits = Sets.newHashSet();
    this.noMoreSplits = false;
    this.executionOptions = executionOptions;
    this.dorisOptions = dorisOptions;
  }

  @Override
  public void start() {

  }

  @Override
  public void pollNext(SourcePipeline<Row> pipeline) throws Exception {
    for (DorisSourceSplit dorisSplit : assignedDorisSplits) {
      DorisSourceSplitReader sourceSplitReader = new DorisSourceSplitReader(dorisSplit, executionOptions,
          dorisOptions);
      while (sourceSplitReader.hasNext()) {
        List<Object> next = sourceSplitReader.next();

        Row row = deserialize(next);
        pipeline.output(row);
      }
      finishSplit(sourceSplitReader);
      finishedDorisSplits.add(dorisSplit);
    }
    assignedDorisSplits.removeAll(finishedDorisSplits);
  }

  public Row deserialize(List<Object> records) {
    Row row = new Row(records.size());
    for (int i = 0; i < records.size(); i++) {
      row.setField(i, records.get(i));
    }
    return row;
  }

  private void finishSplit(DorisSourceSplitReader sourceSplitReader) {
    try {
      sourceSplitReader.close();
    } catch (Exception e) {
      LOG.error("close resource reader failed,", e);
    }
  }

  @Override
  public void addSplits(List<DorisSourceSplit> splits) {
    LOG.info("Subtask {} received {}(s) new splits, splits = {}.", context.getIndexOfSubtask(),
        CollectionUtils.size(splits), splits);
    assignedDorisSplits.addAll(splits);
  }

  @Override
  public boolean hasMoreElements() {
    if (noMoreSplits) {
      return CollectionUtils.size(assignedDorisSplits) != 0;
    }
    return true;
  }

  @Override
  public List<DorisSourceSplit> snapshotState(long checkpointId) {
    return null;
  }

  @Override
  public void close() throws Exception {

  }

  @Override
  public void notifyNoMoreSplits() {
    LOG.info("Subtask {} received no more split signal.", context.getIndexOfSubtask());
    noMoreSplits = true;
  }
}
