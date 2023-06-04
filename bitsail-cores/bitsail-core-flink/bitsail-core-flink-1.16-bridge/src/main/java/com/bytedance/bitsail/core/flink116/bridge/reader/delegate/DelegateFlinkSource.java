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

package com.bytedance.bitsail.core.flink116.bridge.reader.delegate;

import com.bytedance.bitsail.base.connector.reader.v1.SourceSplit;
import com.bytedance.bitsail.base.dirty.AbstractDirtyCollector;
import com.bytedance.bitsail.base.extension.SupportProducedType;
import com.bytedance.bitsail.base.messenger.Messenger;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.model.ColumnInfo;
import com.bytedance.bitsail.common.option.ReaderOptions;
import com.bytedance.bitsail.common.typeinfo.RowTypeInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfoUtils;
import com.bytedance.bitsail.core.flink116.bridge.serializer.DelegateFlinkSourceSplitSerializer;
import com.bytedance.bitsail.core.flink116.bridge.serializer.DelegateSimpleVersionedSerializer;
import com.bytedance.bitsail.flink.core.delagate.translation.BoundednessTranslation;
import com.bytedance.bitsail.flink.core.typeutils.NativeFlinkTypeInfoUtil;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.Serializable;
import java.util.List;

public class DelegateFlinkSource<T, SplitT extends SourceSplit, StateT extends Serializable>
    implements Source<T, DelegateFlinkSourceSplit<SplitT>, StateT>, ResultTypeQueryable<T> {

  public final com.bytedance.bitsail.base.connector.reader.v1.Source<T, SplitT, StateT> source;

  private final BitSailConfiguration commonConfiguration;
  private final BitSailConfiguration readerConfiguration;
  private final RowTypeInfo rowTypeInfo;
  private final AbstractDirtyCollector dirtyCollector;
  private final Messenger messenger;

  public DelegateFlinkSource(com.bytedance.bitsail.base.connector.reader.v1.Source<T, SplitT, StateT> source,
                             BitSailConfiguration commonConfiguration,
                             BitSailConfiguration readerConfiguration,
                             AbstractDirtyCollector dirtyCollector,
                             Messenger messenger) {
    this.source = source;
    this.commonConfiguration = commonConfiguration;
    this.readerConfiguration = readerConfiguration;
    if (source instanceof SupportProducedType) {
      this.rowTypeInfo = ((SupportProducedType) source).getProducedType();
    } else {
      List<ColumnInfo> columnInfos = readerConfiguration
          .get(ReaderOptions.BaseReaderOptions.COLUMNS);

      this.rowTypeInfo = TypeInfoUtils
          .getRowTypeInfo(source.createTypeInfoConverter(), columnInfos);
    }
    this.dirtyCollector = dirtyCollector;
    this.messenger = messenger;
  }

  @Override
  public Boundedness getBoundedness() {
    return BoundednessTranslation.to(source.getSourceBoundedness());
  }

  @Override
  public SourceReader<T, DelegateFlinkSourceSplit<SplitT>> createReader(SourceReaderContext readerContext) throws Exception {
    return new DelegateFlinkSourceReader<>(
        source::createReader,
        readerContext,
        source.getReaderName(),
        rowTypeInfo,
        commonConfiguration,
        readerConfiguration,
        dirtyCollector,
        messenger
    );
  }

  @Override
  public SplitEnumerator<DelegateFlinkSourceSplit<SplitT>, StateT> createEnumerator(SplitEnumeratorContext<DelegateFlinkSourceSplit<SplitT>> enumContext)
      throws Exception {
    return restoreEnumerator(enumContext, null);
  }

  @Override
  public SplitEnumerator<DelegateFlinkSourceSplit<SplitT>, StateT> restoreEnumerator(SplitEnumeratorContext<DelegateFlinkSourceSplit<SplitT>> enumContext,
                                                                                     StateT checkpoint) throws Exception {
    return new DelegateFlinkSourceSplitEnumerator<>(source::createSplitCoordinator, enumContext, checkpoint);
  }

  @Override
  public SimpleVersionedSerializer<DelegateFlinkSourceSplit<SplitT>> getSplitSerializer() {
    return new DelegateFlinkSourceSplitSerializer<>(
        source.getSplitSerializer());
  }

  @Override
  public SimpleVersionedSerializer<StateT> getEnumeratorCheckpointSerializer() {
    return DelegateSimpleVersionedSerializer.delegate(source.getSplitCoordinatorCheckpointSerializer());
  }

  @Override
  public TypeInformation<T> getProducedType() {
    return (TypeInformation<T>) NativeFlinkTypeInfoUtil.getRowTypeInformation(rowTypeInfo);
  }
}
