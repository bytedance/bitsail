/*
 * Copyright 2022-present ByteDance.
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

package com.bytedance.bitsail.connector.legacy.hudi.sink.partitioner;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.index.bucket.BucketIdentifier;

/**
 * Bucket index input partitioner.
 * The fields to hash can be a subset of the primary key fields.
 *
 * @param <T> The type of obj to hash
 */
public class BucketIndexPartitioner<T extends HoodieKey> implements Partitioner<T> {

  private final int bucketNum;
  private final String indexKeyFields;

  public BucketIndexPartitioner(int bucketNum, String indexKeyFields) {
    this.bucketNum = bucketNum;
    this.indexKeyFields = indexKeyFields;
  }

  @Override
  public int partition(HoodieKey key, int numPartitions) {
    int curBucket = BucketIdentifier.getBucketId(key, indexKeyFields, bucketNum);
    int globalHash = (key.getPartitionPath() + curBucket).hashCode() & Integer.MAX_VALUE;
    return BucketIdentifier.mod(globalHash, numPartitions);
  }
}
