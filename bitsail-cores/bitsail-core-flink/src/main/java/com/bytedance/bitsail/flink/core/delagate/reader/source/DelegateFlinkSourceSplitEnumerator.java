/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.bytedance.bitsail.flink.core.delagate.reader.source;

import com.bytedance.bitsail.base.connector.reader.v1.SourceSplit;
import com.bytedance.bitsail.base.connector.reader.v1.SourceSplitCoordinator;
import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.exception.CommonErrorCode;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class DelegateFlinkSourceSplitEnumerator<SplitT extends SourceSplit,
    StateT> implements SplitEnumerator<DelegateFlinkSourceSplit<SplitT>, StateT> {

  private SourceSplitCoordinator<SplitT, StateT> coordinator;

  public DelegateFlinkSourceSplitEnumerator(SourceSplitCoordinator<SplitT, StateT> coordinator) {
    this.coordinator = coordinator;
  }

  @Override
  public void start() {
    coordinator.start();
  }

  @Override
  public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
    coordinator.handleSplitRequest(subtaskId, requesterHostname);
  }

  @Override
  public void addSplitsBack(List<DelegateFlinkSourceSplit<SplitT>> splits, int subtaskId) {
    coordinator.addSplitsBack(splits.stream()
        .map(DelegateFlinkSourceSplit::getSourceSplit)
        .collect(Collectors.toList()), subtaskId);
  }

  @Override
  public void addReader(int subtaskId) {
    coordinator.addReader(subtaskId);
  }

  @Override
  public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
    if (sourceEvent instanceof DelegateSourceEvent) {
      coordinator.handleSourceEvent(subtaskId,
          ((DelegateSourceEvent) sourceEvent).getSourceEvent());
    }
    throw BitSailException.asBitSailException(CommonErrorCode.RUNTIME_ERROR,
        "Source event in not instanceof delegate source event, it's always runtime bug.");
  }

  @Override
  public StateT snapshotState() throws Exception {
    return coordinator.snapshotState();
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) throws Exception {
    coordinator.notifyCheckpointComplete(checkpointId);
  }

  @Override
  public void close() throws IOException {
    coordinator.close();
  }
}
