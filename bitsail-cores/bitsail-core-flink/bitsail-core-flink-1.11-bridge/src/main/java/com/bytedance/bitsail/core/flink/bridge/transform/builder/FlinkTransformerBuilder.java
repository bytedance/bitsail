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

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.option.ReaderOptions;
import com.bytedance.bitsail.core.flink.bridge.transform.delegate.DelegateFlinkPartitioner;
import com.bytedance.bitsail.core.flink.bridge.transform.delegate.RowStringKeySelector;
import com.bytedance.bitsail.flink.core.transform.FlinkDataTransformDAGBuilder;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;

public class FlinkTransformerBuilder<T> extends FlinkDataTransformDAGBuilder<T> {

  BitSailConfiguration jobConf;

  DelegateFlinkPartitioner<T> partitioner;

  KeySelector keySelector;

  public FlinkTransformerBuilder(BitSailConfiguration jobConf) {
    this.jobConf = jobConf;
    this.partitioner = new DelegateFlinkPartitioner<>(jobConf);
    this.keySelector = new RowStringKeySelector(RowStringKeySelector.DEFAULT_KEY_INDEX);
  }

  @Override
  public String getTransformName() {
    return "transform";
  }

  @Override
  public DataStream<T> addTransformer(DataStream<T> source) {
    if (jobConf.get(ReaderOptions.BaseReaderOptions.ENABLE_PARTITION)) {
      return source.partitionCustom(partitioner, keySelector).keyBy(keySelector);
    }
    return source;
  }
}
