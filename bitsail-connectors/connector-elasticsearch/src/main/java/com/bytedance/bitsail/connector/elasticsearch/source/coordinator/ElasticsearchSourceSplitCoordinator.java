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

package com.bytedance.bitsail.connector.elasticsearch.source.coordinator;

import com.bytedance.bitsail.base.connector.reader.v1.SourceSplitCoordinator;
import com.bytedance.bitsail.base.connector.writer.v1.state.EmptyState;
import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.connector.elasticsearch.error.ElasticsearchErrorCode;
import com.bytedance.bitsail.connector.elasticsearch.option.ElasticsearchReaderOptions;
import com.bytedance.bitsail.connector.elasticsearch.rest.EsRestClientBuilder;
import com.bytedance.bitsail.connector.elasticsearch.source.reader.selector.HashReaderSelector;
import com.bytedance.bitsail.connector.elasticsearch.source.reader.selector.ReaderSelector;
import com.bytedance.bitsail.connector.elasticsearch.source.reader.selector.RoundRobinReaderSelector;
import com.bytedance.bitsail.connector.elasticsearch.source.split.ElasticsearchSourceSplit;
import com.bytedance.bitsail.connector.elasticsearch.source.split.ElasticsearchSplitByIndexStrategy;
import com.bytedance.bitsail.connector.elasticsearch.source.split.ElasticsearchSplitStrategy;

import com.google.common.collect.Maps;
import lombok.Getter;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.bytedance.bitsail.connector.elasticsearch.source.reader.selector.SelectorType.HASH;
import static com.bytedance.bitsail.connector.elasticsearch.source.reader.selector.SelectorType.ROUND_ROBIN;

public class ElasticsearchSourceSplitCoordinator implements SourceSplitCoordinator<ElasticsearchSourceSplit, EmptyState> {

  private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchSourceSplitCoordinator.class);

  private final SourceSplitCoordinator.Context<ElasticsearchSourceSplit, EmptyState> context;

  private final BitSailConfiguration jobConf;

  @Getter
  private final Map<Integer, List<ElasticsearchSourceSplit>> splitAssignmentPlan;

  private RestHighLevelClient restClient;

  private ReaderSelector selector;

  public ElasticsearchSourceSplitCoordinator(SourceSplitCoordinator.Context<ElasticsearchSourceSplit, EmptyState> context,
                                             BitSailConfiguration jobConf) {
    this.context = context;
    this.jobConf = jobConf;
    this.splitAssignmentPlan = Maps.newConcurrentMap();
  }

  @Override
  public void start() {
    this.restClient = new EsRestClientBuilder(this.jobConf).build();

    ElasticsearchSplitStrategy elasticsearchSplitStrategy = new ElasticsearchSplitByIndexStrategy(restClient);
    List<ElasticsearchSourceSplit> splitList = elasticsearchSplitStrategy.getElasticsearchSplits(jobConf);

    int readerNum = context.totalParallelism();
    LOG.info("Found {} readers and {} splits.", readerNum, splitList.size());
    if (readerNum > splitList.size()) {
      LOG.error("Reader number {} is larger than split number {}.", readerNum, splitList.size());
    }

    selector = getReaderSelector(jobConf, readerNum);
    for (ElasticsearchSourceSplit split : splitList) {
      int readerIndex = selector.getReaderIndex(split.getIndex());
      splitAssignmentPlan.computeIfAbsent(readerIndex, r -> new ArrayList<>()).add(split);
      LOG.info("Will assign split {} to the {}-th reader.", split.uniqSplitId(), readerIndex);
    }
  }

  @Override
  public void addReader(int subtaskId) {
    LOG.info("Found reader {}.", subtaskId);
    tryToAssignSplitsToReader(subtaskId);
  }

  @Override
  public void addSplitsBack(List<ElasticsearchSourceSplit> splits, int subtaskId) {
    LOG.info("Source reader {} return splits {}.", subtaskId, splits);

    for (ElasticsearchSourceSplit split : splits) {
      int readerIndex = selector.getReaderIndex(split.getIndex());
      splitAssignmentPlan.computeIfAbsent(readerIndex, r -> new ArrayList<>()).add(split);
      LOG.info("Re-assign split {} to the {}-th reader.", split.uniqSplitId(), readerIndex);
    }

    tryToAssignSplitsToReader(subtaskId);
  }

  private void tryToAssignSplitsToReader(int readerIndex) {
    List<ElasticsearchSourceSplit> splits = splitAssignmentPlan.get(readerIndex);
    if (CollectionUtils.isEmpty(splits) && !context.registeredReaders().contains(readerIndex)) {
      return;
    }

    LOG.info("Try assigning splits reader {}, splits are: [{}]", readerIndex,
        splits.stream().map(ElasticsearchSourceSplit::uniqSplitId).collect(Collectors.toList()));
    splitAssignmentPlan.remove(readerIndex);
    context.assignSplit(readerIndex, splits);
    context.signalNoMoreSplits(readerIndex);
    LOG.info("Finish assign splits for reader {}.", readerIndex);
  }

  @Override
  public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
    // empty
  }

  @Override
  public EmptyState snapshotState() throws Exception {
    return new EmptyState();
  }

  @Override
  public void close() {
    try {
      this.restClient.close();
    } catch (IOException e) {
      LOG.error("Fail to close {}.", this.restClient.getClass().getSimpleName(), e);
    }
  }

  private ReaderSelector getReaderSelector(BitSailConfiguration jobConf, int readerNum) {
    String splitStrategy = jobConf.getUnNecessaryOption(
        ElasticsearchReaderOptions.SPLIT_STRATEGY, "round_robin");
    String upper = StringUtils.upperCase(splitStrategy);
    ReaderSelector selector = null;
    if (ROUND_ROBIN.name().equals(upper)) {
      selector = new RoundRobinReaderSelector(readerNum);
    } else if (HASH.name().equals(upper)) {
      selector =  new HashReaderSelector(readerNum);
    } else {
      throw BitSailException.asBitSailException(
          ElasticsearchErrorCode.NOT_SUPPORT_SPLIT_STRATEGY,
          String.format("Split strategy %s is not supported yet.", splitStrategy));
    }

    LOG.info("Select {} as split strategy.", selector.toString());
    return selector;
  }
}
