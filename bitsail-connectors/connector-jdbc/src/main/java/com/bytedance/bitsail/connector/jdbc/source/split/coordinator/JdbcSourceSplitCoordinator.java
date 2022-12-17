/*
 *     Licensed to the Apache Software Foundation (ASF) under one or more
 *     contributor license agreements.  See the NOTICE file distributed with
 *     this work for additional information regarding copyright ownership.
 *     The ASF licenses this file to You under the Apache License, Version 2.0
 *     (the "License"); you may not use this file except in compliance with
 *     the License.  You may obtain a copy of the License at
 *        http://www.apache.org/licenses/LICENSE-2.0
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 */

package com.bytedance.bitsail.connector.jdbc.source.split.coordinator;

import com.bytedance.bitsail.base.connector.reader.v1.SourceSplitCoordinator;
import com.bytedance.bitsail.base.connector.writer.v1.state.EmptyState;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.connector.jdbc.source.split.JdbcSourceSplit;

import com.google.common.collect.Maps;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class JdbcSourceSplitCoordinator implements SourceSplitCoordinator<JdbcSourceSplit, EmptyState> {

  private final SourceSplitCoordinator.Context<JdbcSourceSplit, EmptyState> context;
  private final BitSailConfiguration jobConf;
  private final Map<Integer, Set<JdbcSourceSplit>> splitAssignmentPlan;

  public JdbcSourceSplitCoordinator(SourceSplitCoordinator.Context<JdbcSourceSplit, EmptyState> context,
                                    BitSailConfiguration jobConf) {
    this.context = context;
    this.jobConf = jobConf;
    this.splitAssignmentPlan = Maps.newConcurrentMap();
  }

  @Override
  public void start() {

  }

  @Override
  public void addReader(int subtaskId) {

  }

  @Override
  public void addSplitsBack(List<JdbcSourceSplit> splits, int subtaskId) {

  }

  @Override
  public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {

  }

  @Override
  public EmptyState snapshotState() throws Exception {
    return null;
  }

  @Override
  public void close() {

  }
}
