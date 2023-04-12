/*
 *       Copyright 2022-2023 Bytedance Ltd. and/or its affiliates.
 *
 *       Licensed under the Apache License, Version 2.0 (the "License");
 *       you may not use this file except in compliance with the License.
 *       You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *       Unless required by applicable law or agreed to in writing, software
 *       distributed under the License is distributed on an "AS IS" BASIS,
 *       WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *       See the License for the specific language governing permissions and
 *       limitations under the License.
 */

package com.bytedance.bitsail.core.flink.bridge.reader.builder;

import com.bytedance.bitsail.base.connector.reader.v1.Boundedness;
import com.bytedance.bitsail.base.connector.reader.v1.Source;
import com.bytedance.bitsail.base.connector.reader.v1.SourcePipeline;
import com.bytedance.bitsail.base.connector.reader.v1.SourceReader;
import com.bytedance.bitsail.base.connector.reader.v1.SourceSplit;
import com.bytedance.bitsail.base.connector.reader.v1.SourceSplitCoordinator;
import com.bytedance.bitsail.base.execution.ExecutionEnviron;
import com.bytedance.bitsail.base.extension.ParallelismComputable;
import com.bytedance.bitsail.base.parallelism.ParallelismAdvice;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;

import lombok.NoArgsConstructor;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

public class MockSource implements Source<Void, MockSource.MockSourceSplit, String>, ParallelismComputable {

  @Override
  public void configure(ExecutionEnviron execution, BitSailConfiguration readerConfiguration) {

  }

  @Override
  public Boundedness getSourceBoundedness() {
    return Boundedness.BOUNDEDNESS;
  }

  @Override
  public SourceReader<Void, MockSourceSplit> createReader(SourceReader.Context readerContext) {
    return new SourceReader<Void, MockSourceSplit>() {
      @Override
      public void start() {

      }

      @Override
      public void pollNext(SourcePipeline<Void> pipeline) {

      }

      @Override
      public void addSplits(List<MockSourceSplit> splits) {

      }

      @Override
      public boolean hasMoreElements() {
        return false;
      }

      @Override
      public List<MockSourceSplit> snapshotState(long checkpointId) {
        return new ArrayList<>();
      }

      @Override
      public void close() {

      }
    };
  }

  @Override
  public SourceSplitCoordinator<MockSourceSplit, String> createSplitCoordinator(SourceSplitCoordinator.Context<MockSourceSplit, String> coordinatorContext) {
    return new SourceSplitCoordinator<MockSourceSplit, String>() {
      @Override
      public void start() {

      }

      @Override
      public void addReader(int subtaskId) {

      }

      @Override
      public void addSplitsBack(List<MockSourceSplit> splits, int subtaskId) {

      }

      @Override
      public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {

      }

      @Override
      public String snapshotState(long checkpoint) {
        return null;
      }

      @Override
      public void close() {

      }
    };
  }

  @Override
  public String getReaderName() {
    return "mock-source-for-test";
  }

  @Override
  public ParallelismAdvice getParallelismAdvice(BitSailConfiguration commonConf, BitSailConfiguration selfConf, ParallelismAdvice upstreamAdvice) throws Exception {
    return ParallelismAdvice.builder()
        .adviceParallelism(1)
        .enforceDownStreamChain(false)
        .build();
  }

  @NoArgsConstructor
  public static class MockSourceSplit implements SourceSplit {
    @Override
    public String uniqSplitId() {
      return "";
    }
  }
}
