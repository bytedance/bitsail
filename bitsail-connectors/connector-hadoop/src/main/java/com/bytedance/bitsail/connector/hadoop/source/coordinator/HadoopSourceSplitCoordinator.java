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

package com.bytedance.bitsail.connector.hadoop.source.coordinator;

import com.bytedance.bitsail.base.connector.reader.v1.SourceSplitCoordinator;
import com.bytedance.bitsail.base.connector.writer.v1.state.EmptyState;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.connector.hadoop.source.split.HadoopSourceSplit;

import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

public class HadoopSourceSplitCoordinator implements SourceSplitCoordinator<HadoopSourceSplit, EmptyState> {
  private static final Logger LOG = LoggerFactory.getLogger(HadoopSourceSplitCoordinator.class);
  private final Context<HadoopSourceSplit, EmptyState> coordinatorContext;
  private final BitSailConfiguration readerConfiguration;
  private final HashSet<HadoopSourceSplit> assignedHadoopSplits;
  private final List<String> hadoopPathList;
  private HashSet<HadoopSourceSplit> pendingHadoopSplits;

  public HadoopSourceSplitCoordinator(BitSailConfiguration readerConfiguration,
                                             Context<HadoopSourceSplit, EmptyState> coordinatorContext, List<String> hadoopPathList) {
    this.coordinatorContext = coordinatorContext;
    this.readerConfiguration = readerConfiguration;
    this.hadoopPathList = hadoopPathList;
    this.assignedHadoopSplits = Sets.newHashSet();
  }

  @Override
  public void start() {
    this.pendingHadoopSplits = Sets.newHashSet();
    hadoopPathList.forEach(k -> pendingHadoopSplits.add(new HadoopSourceSplit(k)));
    int readerNum = coordinatorContext.totalParallelism();
    LOG.info("Found {} readers and {} splits.", readerNum, pendingHadoopSplits.size());
    if (readerNum > pendingHadoopSplits.size()) {
      LOG.error("Reader number {} is larger than split number {}.", readerNum, pendingHadoopSplits.size());
    }
  }

  @Override
  public void addReader(int subtaskId) {
    LOG.info("Found reader {}", subtaskId);
    assignSplit(subtaskId);
  }

  @Override
  public void addSplitsBack(List<HadoopSourceSplit> splits, int subtaskId) {
    if (!splits.isEmpty()) {
      LOG.info("Source reader {} return splits {}.", subtaskId, splits);
      pendingHadoopSplits.addAll(splits);
      assignSplit(subtaskId);
    }
  }

  private void assignSplit(int subtaskId) {
    ArrayList<HadoopSourceSplit> currentTaskSplits = new ArrayList<>();
    int readerNum = coordinatorContext.totalParallelism();
    if (readerNum == 1) {
      // if parallelism == 1, we should assign all the splits to reader
      currentTaskSplits.addAll(pendingHadoopSplits);
    } else {
      // if parallelism > 1, according to hashCode of splitId to determine which reader to allocate the current task
      for (HadoopSourceSplit hadoopSourceSplit : pendingHadoopSplits) {
        int readerIndex = getReaderIndex(hadoopSourceSplit.uniqSplitId(), readerNum);
        if (readerIndex == subtaskId) {
          currentTaskSplits.add(hadoopSourceSplit);
        }
      }
    }
    // assign splits
    coordinatorContext.assignSplit(subtaskId, currentTaskSplits);
    // save the state of assigned splits
    assignedHadoopSplits.addAll(currentTaskSplits);
    // remove the assigned splits from pending splits
    currentTaskSplits.forEach(split -> pendingHadoopSplits.remove(split));
    LOG.info("SubTask {} is assigned to [{}]", subtaskId, currentTaskSplits.stream().map(HadoopSourceSplit::uniqSplitId).collect(Collectors.joining(",")));
    coordinatorContext.signalNoMoreSplits(subtaskId);
    LOG.info("Finish assigning splits reader {}", subtaskId);
  }

  private int getReaderIndex(String splitId, int totalReaderNum) {
    return (splitId.hashCode() & Integer.MAX_VALUE) % totalReaderNum;
  }

  @Override
  public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {

  }

  @Override
  public EmptyState snapshotState() throws Exception {
    return new EmptyState();
  }

  @Override
  public void close() {

  }
}
