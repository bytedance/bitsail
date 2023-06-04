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
import com.bytedance.bitsail.base.execution.Mode;
import com.bytedance.bitsail.base.extension.ParallelismComputable;
import com.bytedance.bitsail.base.extension.SupportProducedType;
import com.bytedance.bitsail.base.parallelism.ParallelismAdvice;
import com.bytedance.bitsail.base.serializer.BinarySerializer;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.common.type.BitSailTypeInfoConverter;
import com.bytedance.bitsail.common.type.TypeInfoConverter;
import com.bytedance.bitsail.common.typeinfo.RowTypeInfo;
import com.bytedance.bitsail.component.format.debezium.deserialization.DebeziumDeserializationSchema;
import com.bytedance.bitsail.connector.cdc.schema.DebeziumDeserializationFactory;
import com.bytedance.bitsail.connector.cdc.source.coordinator.CDCSourceSplitCoordinator;
import com.bytedance.bitsail.connector.cdc.source.coordinator.state.AssignmentStateSerializer;
import com.bytedance.bitsail.connector.cdc.source.coordinator.state.BaseAssignmentState;
import com.bytedance.bitsail.connector.cdc.source.split.BaseCDCSplit;
import com.bytedance.bitsail.connector.cdc.source.split.BaseSplitSerializer;

import java.io.IOException;

/**
 * Source to read mysql binlog.
 */
public abstract class BaseCDCSource implements Source<Row, BaseCDCSplit, BaseAssignmentState>, ParallelismComputable, SupportProducedType {

  protected BitSailConfiguration commonConf;
  protected BitSailConfiguration readerConf;

  protected BaseSplitSerializer splitSerializer;
  protected DebeziumDeserializationSchema deserializationSchema;
  protected Boundedness boundedness;

  @Override
  public void configure(ExecutionEnviron execution, BitSailConfiguration readerConfiguration) throws IOException {
    this.boundedness = Mode.STREAMING.equals(execution.getMode()) ?
        Boundedness.UNBOUNDEDNESS :
        Boundedness.BOUNDEDNESS;
    this.readerConf = readerConfiguration;
    this.commonConf = execution.getCommonConfiguration();
    this.splitSerializer = createSplitSerializer();
    this.deserializationSchema = DebeziumDeserializationFactory.getDebeziumDeserializationSchema(readerConf, createTypeInfoConverter());

  }

  @Override
  public Boundedness getSourceBoundedness() {
    return boundedness;
  }

  public abstract BaseSplitSerializer createSplitSerializer();

  @Override
  public abstract SourceReader<Row, BaseCDCSplit> createReader(SourceReader.Context readerContext);

  @Override
  public SourceSplitCoordinator<BaseCDCSplit, BaseAssignmentState> createSplitCoordinator(
      SourceSplitCoordinator.Context<BaseCDCSplit, BaseAssignmentState> coordinatorContext) {
    return new CDCSourceSplitCoordinator(coordinatorContext, readerConf);
  }

  @Override
  public BinarySerializer<BaseCDCSplit> getSplitSerializer() {
    return splitSerializer;
  }

  @Override
  public BinarySerializer<BaseAssignmentState> getSplitCoordinatorCheckpointSerializer() {
    return new AssignmentStateSerializer();
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

  @Override
  public RowTypeInfo getProducedType() {
    if (deserializationSchema instanceof SupportProducedType) {
      return ((SupportProducedType) deserializationSchema).getProducedType();
    }
    return null;
  }
}
