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

package com.bytedance.bitsail.core.flink.bridge.reader.delegate.operator;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.streaming.api.operators.SourceOperator;
import org.apache.flink.streaming.api.operators.SourceOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.function.FunctionWithException;

public class DelegateSourceOperatorFactory<OUT> extends SourceOperatorFactory<OUT> {

  private Source<OUT, ?, ?> source;

  private WatermarkStrategy<OUT> watermarkStrategy;

  public DelegateSourceOperatorFactory(Source<OUT, ?, ?> source,
                                       WatermarkStrategy<OUT> watermarkStrategy) {
    this(source, watermarkStrategy, 1);
  }

  public DelegateSourceOperatorFactory(Source<OUT, ?, ?> source,
                                       WatermarkStrategy<OUT> watermarkStrategy,
                                       int numCoordinatorWorkerThread) {
    super(source, watermarkStrategy, numCoordinatorWorkerThread);
    this.source = source;
    this.watermarkStrategy = watermarkStrategy;
  }

  @Override
  public <T extends StreamOperator<OUT>> T createStreamOperator(
      StreamOperatorParameters<OUT> parameters) {
    final OperatorID operatorId = parameters.getStreamConfig().getOperatorID();
    final OperatorEventGateway gateway =
        parameters.getOperatorEventDispatcher().getOperatorEventGateway(operatorId);

    final SourceOperator<OUT, ?> sourceOperator =
        instantiateSourceOperator(

            source::createReader,
            gateway,
            source.getSplitSerializer(),
            watermarkStrategy,
            parameters.getProcessingTimeService(),
            parameters
                .getContainingTask()
                .getEnvironment()
                .getTaskManagerInfo()
                .getConfiguration(),
            parameters
                .getContainingTask()
                .getEnvironment()
                .getTaskManagerInfo()
                .getTaskManagerExternalAddress());

    sourceOperator.setup(
        parameters.getContainingTask(),
        parameters.getStreamConfig(),
        parameters.getOutput());
    parameters.getOperatorEventDispatcher().registerEventHandler(operatorId, sourceOperator);

    // today's lunch is generics spaghetti
    @SuppressWarnings("unchecked") final T castedOperator = (T) sourceOperator;

    return castedOperator;
  }

  private static <T, SplitT extends SourceSplit> SourceOperator<T, SplitT> instantiateSourceOperator(
      FunctionWithException<SourceReaderContext, SourceReader<T, ?>, Exception>
          readerFactory,
      OperatorEventGateway eventGateway,
      SimpleVersionedSerializer<?> splitSerializer,
      WatermarkStrategy<T> watermarkStrategy,
      ProcessingTimeService timeService,
      Configuration config,
      String localHostName) {

    // jumping through generics hoops: cast the generics away to then cast them back more
    // strictly typed
    final FunctionWithException<SourceReaderContext, SourceReader<T, SplitT>, Exception>
        typedReaderFactory =
        (FunctionWithException<
            SourceReaderContext, SourceReader<T, SplitT>, Exception>)
            (FunctionWithException<?, ?, ?>) readerFactory;

    final SimpleVersionedSerializer<SplitT> typedSplitSerializer =
        (SimpleVersionedSerializer<SplitT>) splitSerializer;

    return new DelegateSourceOperator<>(
        typedReaderFactory,
        eventGateway,
        typedSplitSerializer,
        watermarkStrategy,
        timeService,
        config,
        localHostName);
  }
}
