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

package com.bytedance.bitsail.connector.cdc.source;

import com.bytedance.bitsail.base.connector.reader.v1.Boundedness;
import com.bytedance.bitsail.base.connector.reader.v1.Source;
import com.bytedance.bitsail.base.connector.reader.v1.SourceReader;
import com.bytedance.bitsail.base.connector.reader.v1.SourceSplitCoordinator;
import com.bytedance.bitsail.base.execution.ExecutionEnviron;
import com.bytedance.bitsail.base.extension.ParallelismComputable;
import com.bytedance.bitsail.base.parallelism.ParallelismAdvice;
import com.bytedance.bitsail.base.serializer.BinarySerializer;
import com.bytedance.bitsail.base.serializer.SimpleVersionedBinarySerializer;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.common.type.BitSailTypeInfoConverter;
import com.bytedance.bitsail.common.type.TypeInfoConverter;
import com.bytedance.bitsail.connector.cdc.source.coordinator.CDCSourceSplitCoordinator;
import com.bytedance.bitsail.connector.cdc.source.coordinator.state.BaseAssignmentState;
import com.bytedance.bitsail.connector.cdc.source.split.BinlogSplit;

import java.io.IOException;

/**
 * WIP: Source to read mysql binlog.
 */
public abstract class BinlogSource implements Source<Row, BinlogSplit, BaseAssignmentState>, ParallelismComputable {

  protected BitSailConfiguration jobConf;

  @Override
  public void configure(ExecutionEnviron execution, BitSailConfiguration readerConfiguration) throws IOException {
    this.jobConf = readerConfiguration;
  }

  @Override
  public Boundedness getSourceBoundedness() {
    //TODO: support batch
    return Boundedness.UNBOUNDEDNESS;
  }

  @Override
  public abstract SourceReader<Row, BinlogSplit> createReader(SourceReader.Context readerContext);

  @Override
  public SourceSplitCoordinator<BinlogSplit, BaseAssignmentState> createSplitCoordinator(
      SourceSplitCoordinator.Context<BinlogSplit, BaseAssignmentState> coordinatorContext) {
    return new CDCSourceSplitCoordinator(coordinatorContext, jobConf);
  }

  @Override
  public BinarySerializer<BinlogSplit> getSplitSerializer() {
    return new SimpleVersionedBinarySerializer<>();
  }

  @Override
  public BinarySerializer<BaseAssignmentState> getSplitCoordinatorCheckpointSerializer() {
    return new SimpleVersionedBinarySerializer<>();
  }

  @Override
  public TypeInfoConverter createTypeInfoConverter() {
    return new BitSailTypeInfoConverter();
  }

  @Override
  public ParallelismAdvice getParallelismAdvice(BitSailConfiguration commonConf, BitSailConfiguration selfConf, ParallelismAdvice upstreamAdvice) throws Exception {
    return ParallelismAdvice.builder()
        //Currently only support single parallelism binlog reader
        .adviceParallelism(1)
        .enforceDownStreamChain(false)
        .build();
  }
}
