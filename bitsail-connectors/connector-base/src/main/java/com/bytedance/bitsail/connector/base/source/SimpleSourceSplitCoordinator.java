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

package com.bytedance.bitsail.connector.base.source;

import com.bytedance.bitsail.base.connector.reader.v1.SourceSplitCoordinator;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;

import javax.annotation.Nullable;

import java.util.List;

public class SimpleSourceSplitCoordinator
    implements SourceSplitCoordinator<SimpleSourceSplit, SimpleSourceState> {

  private BitSailConfiguration readerConfiguration;

  private SourceSplitCoordinator.Context<SimpleSourceSplit, SimpleSourceState> coordinatorContext;

  public SimpleSourceSplitCoordinator(BitSailConfiguration readerConfiguration,
                                      SourceSplitCoordinator
                                          .Context<SimpleSourceSplit, SimpleSourceState> splitContext) {
    this.readerConfiguration = readerConfiguration;
    this.coordinatorContext = splitContext;
  }

  @Override
  public void start() {

  }

  @Override
  public void addReader(int subtaskId) {

  }

  @Override
  public void addSplitsBack(List<SimpleSourceSplit> splits, int subtaskId) {

  }

  @Override
  public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {

  }

  @Override
  public SimpleSourceState snapshotState(long checkpointId) throws Exception {
    return null;
  }

  @Override
  public void close() {

  }
}
