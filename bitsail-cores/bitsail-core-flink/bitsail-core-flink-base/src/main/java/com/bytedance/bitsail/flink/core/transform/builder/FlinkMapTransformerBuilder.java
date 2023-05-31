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

package com.bytedance.bitsail.flink.core.transform.builder;

import com.bytedance.bitsail.base.connector.transform.v1.MapTransformer;
import com.bytedance.bitsail.base.connector.transform.v1.Transformer;
import com.bytedance.bitsail.flink.core.transform.delegate.DelegateFlinkMapFunction;
import com.bytedance.bitsail.flink.core.typeutils.AutoDetectFlinkTypeInfoUtil;

import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;

public class FlinkMapTransformerBuilder<T> extends FlinkBaseTransformerBuilder<T> {

  public FlinkMapTransformerBuilder(Transformer transformer) {
    super(transformer);
  }

  @Override
  public DataStream<T> addTransformer(DataStream<T> source, int adviceParallelism) {

    return source.map(new DelegateFlinkMapFunction<>(
        commonConfiguration,
        transformerConfiguration,
        (MapTransformer) transformer,
        AutoDetectFlinkTypeInfoUtil.bridgeRowTypeInfo((RowTypeInfo) source.getType()))
    ).name(getTransformName());
  }
}
