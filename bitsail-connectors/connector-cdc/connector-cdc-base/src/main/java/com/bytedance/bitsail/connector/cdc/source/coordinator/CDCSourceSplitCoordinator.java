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

package com.bytedance.bitsail.connector.cdc.source.coordinator;

import com.bytedance.bitsail.base.connector.reader.v1.SourceEvent;
import com.bytedance.bitsail.base.connector.reader.v1.SourceSplitCoordinator;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.connector.cdc.source.event.BinlogCompleteAckEvent;
import com.bytedance.bitsail.connector.cdc.source.event.BinlogCompleteEvent;
import com.bytedance.bitsail.connector.cdc.source.event.BinlogStopReadEvent;
import com.bytedance.bitsail.connector.cdc.source.offset.BinlogOffset;
import com.bytedance.bitsail.connector.cdc.source.split.BinlogSplit;
import com.bytedance.bitsail.connector.cdc.source.state.BinlogOffsetState;

import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CDCSourceSplitCoordinator implements SourceSplitCoordinator<BinlogSplit, BinlogOffsetState> {

  private static final Logger LOG = LoggerFactory.getLogger(CDCSourceSplitCoordinator.class);

  private final SourceSplitCoordinator.Context<BinlogSplit, BinlogOffsetState> context;
  private final BitSailConfiguration jobConf;
  private final Map<Integer, Set<BinlogSplit>> splitAssignmentPlan;

  public CDCSourceSplitCoordinator(SourceSplitCoordinator.Context<BinlogSplit, BinlogOffsetState> context,
                                   BitSailConfiguration jobConf) {
    this.context = context;
    this.jobConf = jobConf;
    this.splitAssignmentPlan = Maps.newConcurrentMap();
  }

  @Override
  public void start() {
    int totalReader = this.context.registeredReaders().size();
    LOG.info("Total registered reader number: {}", totalReader);
    // assign binlog split to reader
    List<BinlogSplit> initialSplit = new ArrayList<>();
    initialSplit.add(createSplit());
    // test assign split to task0
    this.context.assignSplit(0, initialSplit);
  }

  @Override
  public void addReader(int subtaskId) {
    // do not support add reader during the job is running
    context.sendEventToSourceReader(subtaskId, new BinlogStopReadEvent());
  }

  @Override
  public void addSplitsBack(List<BinlogSplit> splits, int subtaskId) {
    LOG.info("Add split back to assignment plan: {}", splits);
    splitAssignmentPlan.get(subtaskId).addAll(splits);
  }

  @Override
  public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
    // currently reader will not request for split
  }

  @Override
  public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
    if (sourceEvent instanceof BinlogCompleteEvent) {
      // receive a binlog complete signal and send back ack event
      String splitId = ((BinlogCompleteEvent) sourceEvent).getSplitId();
      BinlogCompleteAckEvent feedback = new BinlogCompleteAckEvent(splitId);
      context.sendEventToSourceReader(subtaskId, feedback);
    }
  }

  @Override
  public BinlogOffsetState snapshotState() throws Exception {
    // currently store nothing in state
    return new BinlogOffsetState();
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) throws Exception {
    // do nothing
  }

  @Override
  public void close() {
    LOG.info("Closing MysqlSourceSplitCoordinator");
  }

  private BinlogSplit createSplit() {
    BinlogOffset begin = BinlogOffset.earliest();

    BinlogOffset end = BinlogOffset.boundless();

    return BinlogSplit.builder()
        .splitId("binlog")
        .beginOffset(begin)
        .endOffset(end)
        .build();
  }
}
