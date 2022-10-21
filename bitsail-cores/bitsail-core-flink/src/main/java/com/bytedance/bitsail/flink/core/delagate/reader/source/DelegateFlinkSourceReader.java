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

import com.bytedance.bitsail.base.messenger.Messenger;
import com.bytedance.bitsail.base.metrics.MetricManager;
import com.bytedance.bitsail.base.ratelimit.Channel;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.ddl.typeinfo.TypeInfo;
import com.bytedance.bitsail.common.model.ColumnInfo;
import com.bytedance.bitsail.common.option.ReaderOptions;
import com.bytedance.bitsail.flink.core.delagate.converter.FlinkRowConvertSerializer;

import com.google.common.collect.Lists;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class DelegateFlinkSourceReader<T, SplitT extends com.bytedance.bitsail.base.connector.reader.v1.SourceSplit>
    implements SourceReader<T, DelegateFlinkSourceSplit<SplitT>> {

  private final Function<com.bytedance.bitsail.base.connector.reader.v1.SourceReader.Context,
      com.bytedance.bitsail.base.connector.reader.v1.SourceReader<T, SplitT>> sourceReaderFunction;
  private final SourceReaderContext sourceReaderContext;
  private final BitSailConfiguration commonConfiguration;
  private final BitSailConfiguration readerConfiguration;
  private final TypeInfo<?>[] sourceTypeInfos;
  private final List<ColumnInfo> columnInfos;

  private transient com.bytedance.bitsail.base.connector.reader.v1.SourceReader<T, SplitT> sourceReader;
  private transient DelegateSourcePipeline<T> pipeline;
  private transient FlinkRowConvertSerializer flinkRowConvertSerializer;
  private transient CompletableFuture<Void> available;
  private transient Messenger<String> messenger;
  private transient Channel channel;
  private transient MetricManager metricManager;

  public DelegateFlinkSourceReader(
      Function<com.bytedance.bitsail.base.connector.reader.v1.SourceReader.Context,
          com.bytedance.bitsail.base.connector.reader.v1.SourceReader<T, SplitT>> sourceReaderFunction,
      SourceReaderContext sourceReaderContext,
      TypeInfo<?>[] typeInfos,
      BitSailConfiguration commonConfiguration,
      BitSailConfiguration readerConfiguration) {
    this.sourceReaderFunction = sourceReaderFunction;
    this.sourceReaderContext = sourceReaderContext;
    this.commonConfiguration = commonConfiguration;
    this.readerConfiguration = readerConfiguration;
    this.sourceTypeInfos = typeInfos;
    this.columnInfos = readerConfiguration.get(ReaderOptions.BaseReaderOptions.COLUMNS);
    prepareRuntimePlugins();
  }

  private void prepareRuntimePlugins() {
    com.bytedance.bitsail.base.connector.reader.v1.SourceReader.Context context =
        new com.bytedance.bitsail.base.connector.reader.v1.SourceReader.Context() {
          @Override
          public TypeInfo<?>[] getTypeInfos() {
            return sourceTypeInfos;
          }

          @Override
          public int getIndexOfSubtask() {
            return sourceReaderContext.getIndexOfSubtask();
          }

          @Override
          public void sendSplitRequest() {
            sourceReaderContext.sendSplitRequest();
          }
        };
    this.sourceReader = sourceReaderFunction
        .apply(context);
    this.available = new CompletableFuture<>();
    this.flinkRowConvertSerializer = new FlinkRowConvertSerializer(
        sourceTypeInfos,
        columnInfos,
        commonConfiguration);
  }

  @Override
  public void start() {
    this.sourceReader.start();
  }

  @Override
  public InputStatus pollNext(ReaderOutput<T> output) throws Exception {
    getOrCreateSourcePipeline(output);
    if (sourceReader.hasMoreElements()) {
      sourceReader.pollNext(pipeline);
      return InputStatus.MORE_AVAILABLE;
    }
    return InputStatus.END_OF_INPUT;
  }

  private void getOrCreateSourcePipeline(ReaderOutput<T> output) {
    if (Objects.isNull(pipeline)) {
      pipeline = new DelegateSourcePipeline<>(output, flinkRowConvertSerializer);
    }
  }

  @Override
  public List<DelegateFlinkSourceSplit<SplitT>> snapshotState(long checkpointId) {
    List<SplitT> splits = sourceReader.snapshotState(checkpointId);

    List<DelegateFlinkSourceSplit<SplitT>> flinkSourceSplits = Lists.newArrayListWithCapacity(splits.size());
    for (SplitT splitT : splits) {
      flinkSourceSplits.add(new DelegateFlinkSourceSplit<>(splitT));
    }
    return flinkSourceSplits;
  }

  @Override
  public CompletableFuture<Void> isAvailable() {
    //todo understand how available influence lifecycle.
    return available;
  }

  @Override
  public void addSplits(List<DelegateFlinkSourceSplit<SplitT>> splits) {
    List<SplitT> transformSplit = Lists.newArrayListWithCapacity(splits.size());
    for (DelegateFlinkSourceSplit<SplitT> split : splits) {
      transformSplit.add(split.getSourceSplit());
    }
    sourceReader.addSplits(transformSplit);
  }

  @Override
  public void notifyNoMoreSplits() {
    sourceReader.notifyNoMoreSplits();
  }

  @Override
  public void close() throws Exception {
    sourceReader.close();
  }
}
