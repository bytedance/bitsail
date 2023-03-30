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
import com.bytedance.bitsail.connector.cdc.source.offset.BinlogOffsetType;
import com.bytedance.bitsail.connector.cdc.source.split.BinlogSplit;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;

public class CDCSourceSplitCoordinatorTests {
  @Test
  public void testStartNew() {
    BitSailConfiguration jobConf = BitSailConfiguration.newDefault();
    Map<Integer, List<BinlogSplit>> assigned = new HashMap<Integer, List<BinlogSplit>>();
    SourceSplitCoordinator.Context context = getContext(false, null, assigned);
    CDCSourceSplitCoordinator coordinator = new CDCSourceSplitCoordinator(context, jobConf);
    coordinator.start();
    coordinator.addReader(0);
    Assert.assertEquals(1, assigned.get(0).size());
    Assert.assertEquals(BinlogOffsetType.EARLIEST, assigned.get(0).get(0).getBeginOffset().getOffsetType());
    BinlogAssignmentState state = (BinlogAssignmentState) coordinator.snapshotState(1);
    Assert.assertTrue(state.isAssigned());
  }

  @Test
  public void testRestore() {
    BitSailConfiguration jobConf = BitSailConfiguration.newDefault();
    Map<Integer, List<BinlogSplit>> assigned = new HashMap<Integer, List<BinlogSplit>>();
    BinlogAssignmentState state = new BinlogAssignmentState(true);
    SourceSplitCoordinator.Context context = getContext(true, state, assigned);
    CDCSourceSplitCoordinator coordinator = new CDCSourceSplitCoordinator(context, jobConf);
    coordinator.start();
    // no split was assigned
    Assert.assertEquals(0, assigned.size());
    BinlogAssignmentState newState = (BinlogAssignmentState) coordinator.snapshotState(1);
    Assert.assertTrue(newState.isAssigned());
  }

  SourceSplitCoordinator.Context<BinlogSplit, BaseAssignmentState> getContext(
      boolean isRestore, BaseAssignmentState restoredState, Map<Integer, List<BinlogSplit>> assigned) {
    //TODO: Create a general test context
    SourceSplitCoordinator.Context<BinlogSplit, BaseAssignmentState> context = new SourceSplitCoordinator.Context<BinlogSplit, BaseAssignmentState>() {
      @Override
      public boolean isRestored() {
        return isRestore;
      }

      @Override
      public BaseAssignmentState getRestoreState() {
        return restoredState;
      }

      @Override
      public int totalParallelism() {
        return 1;
      }

      @Override
      public Set<Integer> registeredReaders() {
        Set<Integer> registeredReaders = new HashSet<>();
        registeredReaders.add(0);
        return registeredReaders;
      }

      @Override
      public void assignSplit(int subtaskId, List splits) {
        assigned.put(subtaskId, splits);
      }

      @Override
      public void signalNoMoreSplits(int subtask) {

      }

      @Override
      public void sendEventToSourceReader(int subtaskId, SourceEvent event) {

      }

      @Override
      public void runAsync(Callable callable, BiConsumer handler, int initialDelay, long interval) {

      }

      @Override
      public void runAsyncOnce(Callable callable, BiConsumer handler) {

      }
    };
    return context;
  }
}
