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

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.flink.core.delagate.converter.FlinkRowConvertSerializer;

import com.google.common.collect.Lists;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.core.io.InputStatus;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

public class DelegateFlinkSourceReader<T, SplitT extends com.bytedance.bitsail.base.connector.reader.v1.SourceSplit>
    implements SourceReader<T, DelegateFlinkSourceSplit<SplitT>> {

  private com.bytedance.bitsail.base.connector.reader.v1.SourceReader<T, SplitT> sourceReader;

  private BitSailConfiguration commonConfiguration;

  private BitSailConfiguration readerConfiguration;

  private transient DelegateSourcePipeline<T> pipeline;

  private transient FlinkRowConvertSerializer flinkRowConvertSerializer;

  private transient CompletableFuture<Void> available;

  public DelegateFlinkSourceReader(
      com.bytedance.bitsail.base.connector.reader.v1.SourceReader<T, SplitT> sourceReader,
      com.bytedance.bitsail.base.connector.reader.v1.SourceReader.Context context,
      BitSailConfiguration commonConfiguration,
      BitSailConfiguration readerConfiguration) {
    this.sourceReader = sourceReader;
    this.available = new CompletableFuture<>();
    this.commonConfiguration = commonConfiguration;
    this.readerConfiguration = readerConfiguration;

    this.flinkRowConvertSerializer = new FlinkRowConvertSerializer(
        context.getTypeInfos(),
        context.getColumnInfos(),
        this.commonConfiguration);
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
      flinkSourceSplits.add(new DelegateFlinkSourceSplit(splitT));
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
