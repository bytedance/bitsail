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
import com.bytedance.bitsail.connector.cdc.source.coordinator.state.BaseAssignmentState;
import com.bytedance.bitsail.connector.cdc.source.coordinator.state.BinlogAssignmentState;
import com.bytedance.bitsail.connector.cdc.source.event.BinlogCompleteAckEvent;
import com.bytedance.bitsail.connector.cdc.source.event.BinlogCompleteEvent;
import com.bytedance.bitsail.connector.cdc.source.offset.BinlogOffset;
import com.bytedance.bitsail.connector.cdc.source.split.BaseCDCSplit;
import com.bytedance.bitsail.connector.cdc.source.split.BinlogSplit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

public class CDCSourceSplitCoordinator implements SourceSplitCoordinator<BaseCDCSplit, BaseAssignmentState> {

  private static final Logger LOG = LoggerFactory.getLogger(CDCSourceSplitCoordinator.class);

  private final SourceSplitCoordinator.Context<BaseCDCSplit, BaseAssignmentState> context;
  private final BitSailConfiguration jobConf;
  private boolean isBinlogAssigned;

  public CDCSourceSplitCoordinator(SourceSplitCoordinator.Context<BaseCDCSplit, BaseAssignmentState> context,
                                   BitSailConfiguration jobConf) {
    this.context = context;
    this.jobConf = jobConf;
    if (context.isRestored()) {
      BaseAssignmentState restoredState = context.getRestoreState();
      this.isBinlogAssigned = ((BinlogAssignmentState) restoredState).isAssigned();
      LOG.info(String.format("Restore coordinator state, state type is: %s, binlog is assigned: %s",
          restoredState.getType(), this.isBinlogAssigned));
    } else {
      this.isBinlogAssigned = false;
    }
  }

  @Override
  public void start() {
    // do nothing
    LOG.info("CDCSourceSplitCoordinator start");
  }

  @Override
  public void addReader(int subtaskId) {
    if (!this.isBinlogAssigned && context.registeredReaders().contains(subtaskId)) {
      List<BaseCDCSplit> splitList = new ArrayList<>();
      BinlogSplit split = createBinlogSplit(this.jobConf);
      splitList.add(split);
      LOG.info("binlog is not assigned, assigning a new binlog split to reader: " + split.toString());
      this.context.assignSplit(subtaskId, splitList);
      this.context.signalNoMoreSplits(subtaskId);
      this.isBinlogAssigned = true;
    }
  }

  @Override
  public void addSplitsBack(List<BaseCDCSplit> splits, int subtaskId) {
    // do nothing
    LOG.info("Add split back for split {} for subtask {}", splits, subtaskId);
  }

  @Override
  public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
    // currently reader will not request for split
    LOG.info("Received split request from " + subtaskId);
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
  public BaseAssignmentState snapshotState(long checkpoint) {
    // store whether the binlog split was assigned to reader
    return new BinlogAssignmentState(this.isBinlogAssigned);
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) throws Exception {
    // do nothing
    LOG.info("Checkpoint {} completed", checkpointId);
  }

  @Override
  public void close() {
    LOG.info("Closing CDCSourceSplitCoordinator");
  }

  private BinlogSplit createBinlogSplit(BitSailConfiguration jobConf) {
    BinlogOffset begin = BinlogOffset.createFromJobConf(jobConf);

    BinlogOffset end = BinlogOffset.boundless();

    return new BinlogSplit("binlog-0", begin, end);
  }
}
