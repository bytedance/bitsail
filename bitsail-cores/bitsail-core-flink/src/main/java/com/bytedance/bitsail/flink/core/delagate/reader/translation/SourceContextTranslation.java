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

package com.bytedance.bitsail.flink.core.delagate.reader.translation;

import com.bytedance.bitsail.base.connector.reader.v1.Boundedness;
import com.bytedance.bitsail.base.connector.reader.v1.SourceEvent;
import com.bytedance.bitsail.base.connector.reader.v1.SourceReader;
import com.bytedance.bitsail.base.connector.reader.v1.SourceSplit;
import com.bytedance.bitsail.base.connector.reader.v1.SourceSplitCoordinator;
import com.bytedance.bitsail.common.ddl.typeinfo.TypeInfo;
import com.bytedance.bitsail.common.model.ColumnInfo;
import com.bytedance.bitsail.flink.core.delagate.reader.source.DelegateFlinkSourceSplit;
import com.bytedance.bitsail.flink.core.delagate.reader.source.DelegateSourceEvent;

import com.google.common.collect.Maps;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;

import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class SourceContextTranslation {

  public static SourceReader.Context fromReaderContext(SourceReaderContext context,
                                                       Boundedness boundedness,
                                                       TypeInfo<?>[] typeInfos,
                                                       List<ColumnInfo> columnInfos) {
    return new SourceReader.Context() {

      @Override
      public TypeInfo<?>[] getTypeInfos() {
        return typeInfos;
      }

      @Override
      public List<ColumnInfo> getColumnInfos() {
        return columnInfos;
      }

      @Override
      public int getIndexOfSubtask() {
        return context.getIndexOfSubtask();
      }

      @Override
      public Boundedness getBoundedness() {
        return boundedness;
      }

      @Override
      public void sendSplitRequest() {
        context.sendSplitRequest();
      }
    };
  }

  public static <SplitT extends SourceSplit> SourceSplitCoordinator.Context<SplitT> fromSplitEnumeratorContext(
      SplitEnumeratorContext<DelegateFlinkSourceSplit<SplitT>> context) {
    return new SourceSplitCoordinator.Context<SplitT>() {
      @Override
      public int totalParallelism() {
        return context.currentParallelism();
      }

      @Override
      public Set<Integer> registeredReaders() {
        return context.registeredReaders()
            .keySet();
      }

      @Override
      public void assignSplit(int subtaskId, List<SplitT> splits) {
        HashMap<Integer, List<DelegateFlinkSourceSplit<SplitT>>> assignment = Maps.newHashMap();
        assignment.put(subtaskId, splits.stream()
            .map(DelegateFlinkSourceSplit::new)
            .collect(Collectors.toList()));
        context.assignSplits(new SplitsAssignment<>(assignment));
      }

      @Override
      public void signalNoMoreSplits(int subtask) {
        context.signalNoMoreSplits(subtask);
      }

      @Override
      public void sendEventToSourceReader(int subtaskId, SourceEvent event) {
        context.sendEventToSourceReader(subtaskId, new DelegateSourceEvent(event));
      }
    };
  }
}
