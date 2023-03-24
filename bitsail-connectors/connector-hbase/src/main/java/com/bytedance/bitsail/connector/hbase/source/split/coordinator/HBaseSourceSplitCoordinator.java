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

package com.bytedance.bitsail.connector.hbase.source.split.coordinator;

import com.bytedance.bitsail.base.connector.reader.v1.SourceSplitCoordinator;
import com.bytedance.bitsail.base.connector.writer.v1.state.EmptyState;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.connector.hbase.source.split.HBaseSourceSplit;
import com.bytedance.bitsail.connector.hbase.source.split.strategy.SimpleDivideSplitConstructor;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.NoArgsConstructor;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class HBaseSourceSplitCoordinator implements SourceSplitCoordinator<HBaseSourceSplit, EmptyState> {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseSourceSplitCoordinator.class);

  private final SourceSplitCoordinator.Context<HBaseSourceSplit, EmptyState> context;
  private final BitSailConfiguration jobConf;
  private final Map<Integer, Set<HBaseSourceSplit>> splitAssignmentPlan;

  public HBaseSourceSplitCoordinator(Context<HBaseSourceSplit, EmptyState> context,
                                     BitSailConfiguration jobConfig) {
    this.context = context;
    this.jobConf = jobConfig;
    this.splitAssignmentPlan = Maps.newConcurrentMap();
  }

  @Override
  public void start() {
    List<HBaseSourceSplit> splitList;
    try {
      SimpleDivideSplitConstructor constructor = new SimpleDivideSplitConstructor(jobConf);
      splitList = constructor.construct();
    } catch (IOException e) {
      HBaseSourceSplit split = new HBaseSourceSplit(0);
      split.setReadTable(true);
      splitList = Collections.singletonList(split);
      LOG.error("Failed to construct splits, will directly read the table.", e);
    }

    int readerNum = context.totalParallelism();
    LOG.info("Found {} readers and {} splits.", readerNum, splitList.size());
    if (readerNum > splitList.size()) {
      LOG.error("Reader number {} is larger than split number {}.", readerNum, splitList.size());
    }

    for (HBaseSourceSplit split : splitList) {
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
  public void addSplitsBack(List<HBaseSourceSplit> splits, int subtaskId) {
    LOG.info("Source reader {} return splits {}.", subtaskId, splits);

    int readerNum = context.totalParallelism();
    for (HBaseSourceSplit split : splits) {
      int readerIndex = ReaderSelector.getReaderIndex(readerNum);
      splitAssignmentPlan.computeIfAbsent(readerIndex, k -> new HashSet<>()).add(split);
      LOG.info("Re-assign split {} to the {}-th reader.", split.uniqSplitId(), readerIndex);
    }

    tryAssignSplitsToReader();
  }

  @Override
  public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {

  }

  @Override
  public EmptyState snapshotState(long checkpointId) throws Exception {
    return new EmptyState();
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) throws Exception {
    SourceSplitCoordinator.super.notifyCheckpointComplete(checkpointId);
  }

  @Override
  public void close() {

  }

  private void tryAssignSplitsToReader() {
    Map<Integer, List<HBaseSourceSplit>> splitsToAssign = new HashMap<>();

    for (Integer readerIndex : splitAssignmentPlan.keySet()) {
      if (CollectionUtils.isNotEmpty(splitAssignmentPlan.get(readerIndex)) &&
          context.registeredReaders().contains(readerIndex)) {
        splitsToAssign.put(readerIndex, Lists.newArrayList(splitAssignmentPlan.get(readerIndex)));
      }
    }

    for (Integer readerIndex : splitsToAssign.keySet()) {
      LOG.info("Try assigning splits reader {}, splits are: [{}]", readerIndex,
          splitsToAssign
              .get(readerIndex)
              .stream()
              .map(HBaseSourceSplit::uniqSplitId)
              .collect(Collectors.toList()));
      splitAssignmentPlan.remove(readerIndex);
      context.assignSplit(readerIndex, splitsToAssign.get(readerIndex));
      context.signalNoMoreSplits(readerIndex);
      LOG.info("Finish assigning splits reader {}", readerIndex);
    }
  }

  @NoArgsConstructor
  static class ReaderSelector {
    private static long readerIndex = 0;
    public static int getReaderIndex(int totalReaderNum) {
      return (int) readerIndex++ % totalReaderNum;
    }
  }
}
