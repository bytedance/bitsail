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

package com.bytedance.bitsail.core.flink.bridge.transform.builder;

import com.bytedance.bitsail.base.connector.transform.TransformType;
import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.exception.CommonErrorCode;
import com.bytedance.bitsail.common.option.TransformOptions;
import com.bytedance.bitsail.core.flink.bridge.transform.delegate.DelegateFlinkMapFunction;
import com.bytedance.bitsail.core.flink.bridge.transform.delegate.DelegateFlinkPartitioner;
import com.bytedance.bitsail.core.flink.bridge.transform.delegate.RowKeySelector;
import com.bytedance.bitsail.flink.core.transform.FlinkDataTransformDAGBuilder;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.Locale;

public class FlinkTransformerBuilder<T> extends FlinkDataTransformDAGBuilder<T> {

  BitSailConfiguration jobConf;

  DelegateFlinkPartitioner<T> partitioner;

  KeySelector keySelector;

  public FlinkTransformerBuilder(BitSailConfiguration jobConf) {
    this.jobConf = jobConf;
    this.partitioner = new DelegateFlinkPartitioner<>(jobConf);
    this.keySelector = new RowKeySelector<>(RowKeySelector.DEFAULT_KEY_INDEX);
  }

  @Override
  public String getTransformName() {
    return "transform";
  }

  @Override
  public DataStream<T> addTransformer(DataStream<T> source) {
    TransformType transformType = TransformType.valueOf(
        jobConf.get(TransformOptions.BaseTransformOptions.TRANSFORM_TYPE).toUpperCase(Locale.ROOT));

    switch (transformType) {
      case PARTITION_BY:
        return addPartitioner(source);
      case MAP:
        return addMap(source);
      default:
        throw BitSailException.asBitSailException(
            CommonErrorCode.UNSUPPORTED_TRANSFORM_TYPE, String.format("transform type %s is currently unsupported", transformType));
    }
  }

  private DataStream<T> addPartitioner(DataStream<T> source) {
    DelegateFlinkPartitioner<T> partitioner = new DelegateFlinkPartitioner<>(jobConf);
    KeySelector keySelector = new RowKeySelector<>(jobConf.get(
        TransformOptions.BaseTransformOptions.PARTITION_KEY_INDEX));
    return source.partitionCustom(partitioner, keySelector);
  }

  private DataStream<T> addMap(DataStream<T> source) {
    DelegateFlinkMapFunction<T, T> mapFunction = new DelegateFlinkMapFunction<>(jobConf, source.getType());
    return source.map(mapFunction, mapFunction.getOutputType());
  }
}
