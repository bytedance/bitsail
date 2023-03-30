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

package com.bytedance.bitsail.connector.mongodb.source.split;

import com.bytedance.bitsail.base.connector.reader.v1.SourceSplitCoordinator;
import com.bytedance.bitsail.base.connector.writer.v1.state.EmptyState;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.connector.mongodb.config.MongoDBConnConfig;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.NoArgsConstructor;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Coordinator for MongoDB source split.
 */
public class MongoDBSourceSplitCoordinator implements SourceSplitCoordinator<MongoDBSourceSplit, EmptyState> {

  private static final Logger LOG = LoggerFactory.getLogger(MongoDBSourceSplitCoordinator.class);

  private final Context<MongoDBSourceSplit, EmptyState> context;
  private final BitSailConfiguration jobConf;
  private final Map<Integer, Set<MongoDBSourceSplit>> splitAssignmentPlan;

  public MongoDBSourceSplitCoordinator(Context<MongoDBSourceSplit, EmptyState> context,
                                       BitSailConfiguration jobConf) {
    this.context = context;
    this.jobConf = jobConf;
    this.splitAssignmentPlan = Maps.newConcurrentMap();
  }

  @Override
  public void start() {
    MongoDBConnConfig mongoConnConfig = MongoDBConnConfig.initMongoConnConfig(jobConf);
    MongoDBSplitterFactory splitterFactory = MongoDBSplitterFactory.getMongoSplitterFactory(jobConf, mongoConnConfig);
    MongoDBSplitter splitter = splitterFactory.getSplitter(jobConf);
    List<MongoDBSourceSplit> splitList = splitter.getSplits();

    int readerNum = context.totalParallelism();
    LOG.info("Found {} readers and {} splits in mongodb", readerNum, splitList.size());
    if (readerNum > splitList.size()) {
      LOG.error("Reader number {} is larger than split number {}  in mongodb", readerNum, splitList.size());
    }

    for (MongoDBSourceSplit split : splitList) {
      int readerIndex = ReaderSelector.getReaderIndex(readerNum);
      splitAssignmentPlan.computeIfAbsent(readerIndex, k -> new HashSet<>()).add(split);
      LOG.info("Will assign split {} to the {}-th reader", split.uniqSplitId(), readerIndex);
    }
  }

  @Override
  public void addReader(int subtaskId) {
    LOG.info("Found reader {}", subtaskId);
    tryAssignSplitsToReader();
  }

  @Override
  public void addSplitsBack(List<MongoDBSourceSplit> splits, int subtaskId) {
    LOG.info("Source reader {} return splits {}.", subtaskId, splits);

    int readerNum = context.totalParallelism();
    for (MongoDBSourceSplit split : splits) {
      int readerIndex = ReaderSelector.getReaderIndex(readerNum);
      splitAssignmentPlan.computeIfAbsent(readerIndex, k -> new HashSet<>()).add(split);
      LOG.info("Re-assign split {} to the {}-th reader.", split.uniqSplitId(), readerIndex);
    }

    tryAssignSplitsToReader();
  }

  private void tryAssignSplitsToReader() {
    Map<Integer, List<MongoDBSourceSplit>> splitsToAssign = new HashMap<>();

    for (Integer readerIndex : splitAssignmentPlan.keySet()) {
      if (CollectionUtils.isNotEmpty(splitAssignmentPlan.get(readerIndex)) && context.registeredReaders().contains(readerIndex)) {
        splitsToAssign.put(readerIndex, Lists.newArrayList(splitAssignmentPlan.get(readerIndex)));
      }
    }

    for (Integer readerIndex : splitsToAssign.keySet()) {
      splitAssignmentPlan.remove(readerIndex);
      context.assignSplit(readerIndex, splitsToAssign.get(readerIndex));
      context.signalNoMoreSplits(readerIndex);
      LOG.info("Finish assigning splits to reader {}", readerIndex);
    }
  }

  @Override
  public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
    // empty
  }

  @Override
  public EmptyState snapshotState(long checkpointId) {
    return new EmptyState();
  }

  @Override
  public void close() {
    // empty
  }

  @NoArgsConstructor
  static class ReaderSelector {
    private static long readerIndex = 0;

    public static int getReaderIndex(int totalReaderNum) {
      return (int) readerIndex++ % totalReaderNum;
    }
  }
}
