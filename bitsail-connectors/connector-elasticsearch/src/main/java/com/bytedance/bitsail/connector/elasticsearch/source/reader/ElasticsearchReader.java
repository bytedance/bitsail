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

package com.bytedance.bitsail.connector.elasticsearch.source.reader;

import com.bytedance.bitsail.base.connector.reader.v1.SourcePipeline;
import com.bytedance.bitsail.base.connector.reader.v1.SourceReader;
import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.model.ColumnInfo;
import com.bytedance.bitsail.common.option.ReaderOptions;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.connector.elasticsearch.error.ElasticsearchErrorCode;
import com.bytedance.bitsail.connector.elasticsearch.option.ElasticsearchReaderOptions;
import com.bytedance.bitsail.connector.elasticsearch.rest.EsRestClientBuilder;
import com.bytedance.bitsail.connector.elasticsearch.rest.source.EsSourceRequest;
import com.bytedance.bitsail.connector.elasticsearch.source.reader.deserializer.ElasticsearchRowDeserializer;
import com.bytedance.bitsail.connector.elasticsearch.source.split.ElasticsearchSourceSplit;

import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class ElasticsearchReader implements SourceReader<Row, ElasticsearchSourceSplit> {

  private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchReader.class);

  private final int subTaskId;

  private int totalSplitNum = 0;

  private boolean hasNoMoreSplits = false;

  private final Deque<ElasticsearchSourceSplit> splits;

  private final RestHighLevelClient restHighLevelClient;

  private EsSourceRequest esSourceRequest;

  private final List<ColumnInfo> columnInfos;

  private final String scrollTime;

  private final int scrollSize;

  private final List<String> columnNames;

  private final ElasticsearchRowDeserializer deserializer;

  private List<Map<String, Object>> curResult;

  private ElasticsearchSourceSplit curSplit;

  private AtomicInteger readIndex = new AtomicInteger(0);

  public ElasticsearchReader(BitSailConfiguration jobConf, SourceReader.Context readerContext) {
    this.subTaskId = readerContext.getIndexOfSubtask();

    this.columnInfos = jobConf.getNecessaryOption(
        ReaderOptions.BaseReaderOptions.COLUMNS, ElasticsearchErrorCode.REQUIRED_VALUE);
    columnNames = columnInfos.stream().map(ColumnInfo::getName).collect(Collectors.toList());
    this.scrollTime = jobConf.get(ElasticsearchReaderOptions.SCROLL_TIME);
    this.scrollSize = jobConf.get(ElasticsearchReaderOptions.SCROLL_SIZE);

    this.splits = new ConcurrentLinkedDeque<>();

    this.restHighLevelClient = new EsRestClientBuilder(jobConf).build();
    this.esSourceRequest = new EsSourceRequest(this.restHighLevelClient);
    this.deserializer = new ElasticsearchRowDeserializer(
        readerContext.getTypeInfos(), readerContext.getFieldNames(), jobConf);

    LOG.info("Elasticsearch source reader {} is initialized.", subTaskId);
  }

  @Override
  public void start() {
    LOG.info("Task {} started.", subTaskId);
  }

  @Override
  public void pollNext(SourcePipeline<Row> pipeline) throws Exception {
    if (Objects.isNull(curResult) && splits.isEmpty()) {
      return;
    }

    if (Objects.isNull(curResult)) {
      this.curSplit = splits.poll();
      try {
        this.curResult = esSourceRequest.getAllDocuments(
            this.curSplit.getIndex(), columnNames, scrollSize, scrollTime);
      } catch (IOException e) {
        throw BitSailException.asBitSailException(
            ElasticsearchErrorCode.FETCH_DATA_FAILED,
            "Failed to fetch document data."
        );
      }
      LOG.info("Task {} finish read split: {}=[{}]", subTaskId, this.curSplit.getSplitId(), this.curSplit);
    }

    if (this.curResult.size() > 0) {
      for (int i = 0; i < curResult.size(); i++) {
        Map<String, Object> doc = this.curResult.get(this.readIndex.getAndIncrement());
        Row row = deserializer.deserialize(doc);
        pipeline.output(row);
      }
    } else {
      curResult = null;
      LOG.info("Task {} finishes reading rows from split: {}", subTaskId, curSplit.uniqSplitId());
    }
  }

  @Override
  public void addSplits(List<ElasticsearchSourceSplit> splits) {
    totalSplitNum += splits.size();
    this.splits.addAll(splits);
  }

  @Override
  public boolean hasMoreElements() {
    if (hasNoMoreSplits && splits.isEmpty()) {
      LOG.info("Finish reading all {} splits.", totalSplitNum);
      return false;
    }
    return true;
  }

  @Override
  public List<ElasticsearchSourceSplit> snapshotState(long checkpointId) {
    return Collections.emptyList();
  }

  @Override
  public void close() throws Exception {
    this.restHighLevelClient.close();
    LOG.info("Task {} is closed.", subTaskId);
  }

  @Override
  public void notifyNoMoreSplits() {
    this.hasNoMoreSplits = true;
    LOG.info("No more splits will be assigned.");
  }
}
