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

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.source.event.RequestSplitEvent;
import org.apache.flink.runtime.source.event.SourceEventWrapper;
import org.apache.flink.streaming.api.operators.SourceOperator;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.function.FunctionWithException;

import java.lang.reflect.Field;

public class DelegateSourceOperator<OUT, SplitT extends SourceSplit> extends SourceOperator<OUT, SplitT> {

  private FunctionWithException<SourceReaderContext, SourceReader<OUT, SplitT>, Exception> factory;

  private OperatorEventGateway eventGateway;

  private Configuration configuration;

  private String localHostname;

  public DelegateSourceOperator(
      FunctionWithException<SourceReaderContext, SourceReader<OUT, SplitT>, Exception> readerFactory,
      OperatorEventGateway operatorEventGateway,
      SimpleVersionedSerializer<SplitT> splitSerializer,
      WatermarkStrategy<OUT> watermarkStrategy,
      ProcessingTimeService timeService,
      Configuration configuration,
      String localHostname) {
    super(readerFactory, operatorEventGateway, splitSerializer, watermarkStrategy, timeService, configuration, localHostname);
    this.factory = readerFactory;
    this.eventGateway = operatorEventGateway;
    this.configuration = configuration;
    this.localHostname = localHostname;
  }

  @Override
  public void initReader() throws Exception {
    if (getSourceReader() != null) {
      return;
    }

    final MetricGroup metricGroup = getMetricGroup();
    assert metricGroup != null;

    StreamingRuntimeContext runtimeContext = getRuntimeContext();
    final int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();

    final SourceReaderContext context =
        new DelegateSourceReaderContext() {
          @Override
          public MetricGroup metricGroup() {
            return metricGroup;
          }

          @Override
          public Configuration getConfiguration() {
            return configuration;
          }

          @Override
          public String getLocalHostName() {
            return localHostname;
          }

          @Override
          public int getIndexOfSubtask() {
            return subtaskIndex;
          }

          @Override
          public void sendSplitRequest() {
            eventGateway.sendEventToCoordinator(
                new RequestSplitEvent(getLocalHostName()));
          }

          @Override
          public RuntimeContext getRuntimeContext() {
            return runtimeContext;
          }

          @Override
          public void sendSourceEventToCoordinator(SourceEvent event) {
            eventGateway.sendEventToCoordinator(new SourceEventWrapper(event));
          }
        };

    Field sourceReader = FieldUtils.getDeclaredField(SourceOperator.class, "sourceReader", true);
    sourceReader.set(this, factory.apply(context));
  }
}
