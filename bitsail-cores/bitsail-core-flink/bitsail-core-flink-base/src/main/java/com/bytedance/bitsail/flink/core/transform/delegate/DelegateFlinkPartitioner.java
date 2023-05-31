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

import com.bytedance.bitsail.base.connector.transform.v1.PartitionTransformer;

import org.apache.flink.api.common.functions.Partitioner;

public class DelegateFlinkPartitioner<K> implements Partitioner<K> {
  private final PartitionTransformer<?, K> partitionTransformer;

  public DelegateFlinkPartitioner(PartitionTransformer<?, K> partitionTransformer) {
    this.partitionTransformer = partitionTransformer;

  }

  @Override
  public int partition(K key, int numPartitions) {
    return partitionTransformer.partition(key, numPartitions);
  }
}
