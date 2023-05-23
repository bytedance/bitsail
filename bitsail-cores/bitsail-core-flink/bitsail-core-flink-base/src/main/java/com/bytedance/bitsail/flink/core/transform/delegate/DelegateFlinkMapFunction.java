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

package com.bytedance.bitsail.flink.core.transform.delegate;

import com.bytedance.bitsail.base.connector.transform.v1.MapTransformer;
import com.bytedance.bitsail.base.extension.SupportProducedType;
import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.model.ColumnInfo;
import com.bytedance.bitsail.common.option.TransformOptions;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.common.type.BitSailTypeInfoConverter;
import com.bytedance.bitsail.common.typeinfo.RowTypeInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfoUtils;
import com.bytedance.bitsail.flink.core.delagate.converter.FlinkRowConverter;
import com.bytedance.bitsail.flink.core.typeutils.NativeFlinkTypeInfoUtil;

import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class DelegateFlinkMapFunction<I extends Row, O extends org.apache.flink.types.Row>
    extends RichMapFunction<O, O> implements ResultTypeQueryable<O> {

  private static final Logger LOG = LoggerFactory.getLogger(DelegateFlinkMapFunction.class);

  private final BitSailConfiguration commonConfiguration;
  private final BitSailConfiguration transformConfiguration;
  private final MapTransformer<I> mapTransformer;
  private final RowTypeInfo rowTypeInfo;

  private transient FlinkRowConverter rowConverter;

  public DelegateFlinkMapFunction(BitSailConfiguration commonConfiguration,
                                  BitSailConfiguration transformConfiguration,
                                  MapTransformer<I> mapTransformer,
                                  RowTypeInfo rowTypeInfo) {
    this.commonConfiguration = commonConfiguration;
    this.transformConfiguration = transformConfiguration;
    this.mapTransformer = mapTransformer;

    List<ColumnInfo> columnInfos = transformConfiguration.get(TransformOptions.BaseTransformOptions.COLUMNS);
    if (CollectionUtils.isNotEmpty(columnInfos)) {
      this.rowTypeInfo = TypeInfoUtils.getRowTypeInfo(new BitSailTypeInfoConverter(),
          columnInfos);
    } else if (mapTransformer instanceof SupportProducedType) {
      this.rowTypeInfo = ((SupportProducedType) mapTransformer).getProducedType();
    } else {
      this.rowTypeInfo = rowTypeInfo;
    }
    mapTransformer.setTypeInfo(rowTypeInfo);
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    this.rowConverter = new FlinkRowConverter(
        rowTypeInfo,
        commonConfiguration);
    this.mapTransformer.open();
  }

  @Override
  public O map(O input) throws Exception {
    Row converted;
    try {
      converted = rowConverter.from(input);
    } catch (BitSailException e) {
      LOG.error("Transform {} convert input failed.", mapTransformer.getComponentName(), e);
      //TODO dirty collect
      throw new IllegalArgumentException(e);
    }

    I transformed = mapTransformer.map((I) converted);

    org.apache.flink.types.Row recovered;
    try {
      recovered = rowConverter.to((Row) transformed);
    } catch (BitSailException e) {
      LOG.error("Transform {} convert input failed.", mapTransformer.getComponentName(), e);
      //TODO dirty collect
      throw new IllegalArgumentException(e);
    }
    return (O) recovered;
  }

  @Override
  public TypeInformation<O> getProducedType() {
    return (TypeInformation) NativeFlinkTypeInfoUtil.getRowTypeInformation(rowTypeInfo);
  }
}
