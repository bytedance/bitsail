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

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;

import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

public class DelegateFlinkPartitionerTest {
  @Test
  public void testPartitioner() {
    BitSailConfiguration jobConf = BitSailConfiguration.newDefault();
    DelegateFlinkPartitioner partitioner = new DelegateFlinkPartitioner<>(jobConf);
    int maxParallelism = 4;
    Random random = new Random();
    for (int i = 0; i < 1000000; i++) {
      Assert.assertTrue(isValidPartition(0, maxParallelism, partitioner.partition(random.nextInt(), maxParallelism)));
      Assert.assertTrue(isValidPartition(0, maxParallelism, partitioner.partition(random.nextLong(), maxParallelism)));
      Assert.assertTrue(isValidPartition(0, maxParallelism, partitioner.partition(random.nextDouble(), maxParallelism)));
      Assert.assertTrue(isValidPartition(0, maxParallelism, partitioner.partition(random.nextFloat(), maxParallelism)));
      Assert.assertTrue(isValidPartition(0, maxParallelism, partitioner.partition(String.valueOf(random.nextLong()), maxParallelism)));
    }
  }

  private static boolean isValidPartition(int min, int max, int val) {
    return val >= min && val < max;
  }
}
