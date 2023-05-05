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

package com.bytedance.bitsail.core.flink.bridge.transform.delegate;

import com.bytedance.bitsail.base.connector.transform.PartitionerType;
import com.bytedance.bitsail.base.connector.transform.v1.BitSailPartitioner;
import com.bytedance.bitsail.base.connector.transform.v1.SimpleHashCodePartitioner;
import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.exception.CommonErrorCode;
import com.bytedance.bitsail.common.option.TransformOptions;

import org.apache.flink.api.common.functions.Partitioner;

import java.util.Locale;

public class DelegateFlinkPartitioner<T> implements Partitioner<T> {
  BitSailPartitioner<T> realPartitioner;

  public DelegateFlinkPartitioner(BitSailConfiguration jobConf) {
    this.realPartitioner = createPartitioner(jobConf);
  }

  @Override
  public int partition(T key, int numPartitions) {
    return realPartitioner.partition(key, numPartitions);
  }

  BitSailPartitioner<T> createPartitioner(BitSailConfiguration jobConf) {
    PartitionerType partitionerType = PartitionerType.valueOf(
        jobConf.get(TransformOptions.BaseTransformOptions.PARTITIONER_TYPE).trim().toUpperCase(Locale.ROOT));
    switch (partitionerType) {
      case HASH:
        return new SimpleHashCodePartitioner<>();
      default:
        throw BitSailException.asBitSailException(CommonErrorCode.TRANSFORM_ERROR,
            String.format("partitioner type %s is not supported yet", partitionerType));
    }
  }
}
