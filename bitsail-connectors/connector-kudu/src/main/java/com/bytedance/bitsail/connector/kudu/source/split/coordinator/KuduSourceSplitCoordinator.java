/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.bytedance.bitsail.connector.kudu.source.split.coordinator;

import com.bytedance.bitsail.base.connector.reader.v1.SourceSplitCoordinator;
import com.bytedance.bitsail.base.connector.writer.v1.state.EmptyState;
import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.connector.kudu.core.KuduFactory;
import com.bytedance.bitsail.connector.kudu.error.KuduErrorCode;
import com.bytedance.bitsail.connector.kudu.source.split.AbstractKuduSplitConstructor;
import com.bytedance.bitsail.connector.kudu.source.split.KuduSourceSplit;
import com.bytedance.bitsail.connector.kudu.source.split.KuduSplitFactory;

import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kudu.client.KuduClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class KuduSourceSplitCoordinator implements SourceSplitCoordinator<KuduSourceSplit, EmptyState> {
  private static final Logger LOG = LoggerFactory.getLogger(KuduSourceSplitCoordinator.class);

  private final SourceSplitCoordinator.Context<KuduSourceSplit, EmptyState> context;
  private final BitSailConfiguration jobConf;
  private final Map<Integer, Set<KuduSourceSplit>> splitAssignmentPlan;
  private final Set<KuduSourceSplit> assignedSplits;

  public KuduSourceSplitCoordinator(Context<KuduSourceSplit, EmptyState> context,
                                    BitSailConfiguration jobConf) {
    this.context = context;
    this.jobConf = jobConf;
    this.assignedSplits = new HashSet<>();
    this.splitAssignmentPlan = new HashMap<>();
  }

  @Override
  public void start() {
    List<KuduSourceSplit> splitList;
    try (KuduFactory kuduFactory = new KuduFactory(jobConf, "reader")) {
      KuduClient client = kuduFactory.getClient();
      AbstractKuduSplitConstructor splitConstructor = KuduSplitFactory.getSplitConstructor(jobConf, client);
      splitList = splitConstructor.construct(client);
    } catch (IOException e) {
      throw new BitSailException(KuduErrorCode.SPLIT_ERROR, "Failed to create splits.");
    }

    int readerNum = context.totalParallelism();
    if (readerNum > splitList.size()) {
      LOG.error("Reader number {} is larger than split number {}.", readerNum, splitList.size());
    }

    for (KuduSourceSplit split : splitList) {
      int readerIndex = getReaderIndex(split, readerNum);
      splitAssignmentPlan.computeIfAbsent(readerIndex, HashSet::new).add(split);
      LOG.info("Will assign split {} to the {}-th reader", split.uniqSplitId(), readerIndex);
    }
  }

  @Override
  public void addReader(int subtaskId) {
    LOG.info("Found reader {}", subtaskId);
    tryAssignSplitsToReader();
  }

  @Override
  public void addSplitsBack(List<KuduSourceSplit> splits, int subtaskId) {
    LOG.info("Source reader {} return splits {}.", subtaskId, splits);

    int numReader = context.totalParallelism();
    for (KuduSourceSplit split : splits) {
      int readerIndex = getReaderIndex(split, numReader);
      splitAssignmentPlan.computeIfAbsent(readerIndex, HashSet::new).add(split);
      LOG.info("Re-assign split {} to the {}-th reader.", split.uniqSplitId(), readerIndex);
    }
  }

  private void tryAssignSplitsToReader() {
    Map<Integer, List<KuduSourceSplit>> splitsToAssign = new HashMap<>();

    for (Integer readerIndex : splitAssignmentPlan.keySet()) {
      if (CollectionUtils.isNotEmpty(splitAssignmentPlan.get(readerIndex)) && context.registeredReaders().contains(readerIndex)) {
        splitsToAssign.put(readerIndex, Lists.newArrayList(splitAssignmentPlan.get(readerIndex)));
      }
    }

    for (Integer readerIndex : splitsToAssign.keySet()) {
      LOG.info("Try assigning splits reader {}, splits are: {}", readerIndex, splitsToAssign.get(readerIndex));

      context.assignSplit(readerIndex, splitsToAssign.get(readerIndex));
      context.signalNoMoreSplits(readerIndex);
      Set<KuduSourceSplit> remove = splitAssignmentPlan.remove(readerIndex);
      assignedSplits.addAll(remove);

      LOG.info("Finish assigning splits reader {}", readerIndex);
    }
  }

  private static int getReaderIndex(KuduSourceSplit split, int totalReaderNum) {
    return split.hashCode() % totalReaderNum;
  }

  @Override
  public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
    // empty
  }

  @Override
  public EmptyState snapshotState() {
    return new EmptyState();
  }

  @Override
  public void close() {
    // empty
  }
}
