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

package com.bytedance.bitsail.connector.kafka.source.coordinator;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.connector.base.source.split.SplitAssigner;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class FairKafkaSplitAssigner implements SplitAssigner<TopicPartition> {

  private static final Logger LOG = LoggerFactory.getLogger(FairKafkaSplitAssigner.class);

  private BitSailConfiguration readerConfiguration;

  private AtomicInteger atomicInteger;

  public Map<TopicPartition, String> kafKaSplitIncrementMapping;

  public FairKafkaSplitAssigner(BitSailConfiguration readerConfiguration,
                                Map<TopicPartition, String> kafkaSplitIncrementMapping) {
    this.readerConfiguration = readerConfiguration;
    this.kafKaSplitIncrementMapping = kafkaSplitIncrementMapping;
    this.atomicInteger = new AtomicInteger(CollectionUtils
        .size(kafkaSplitIncrementMapping.keySet()));
  }

  @Override
  public String assignSplitId(TopicPartition topicPartition) {
    if (!kafKaSplitIncrementMapping.containsKey(topicPartition)) {
      kafKaSplitIncrementMapping.put(topicPartition, String.valueOf(atomicInteger.getAndIncrement()));
    }
    return kafKaSplitIncrementMapping.get(topicPartition);
  }

  @Override
  public int assignToReader(String splitId, int totalParallelism) {
    return (splitId.hashCode() & Integer.MAX_VALUE) % totalParallelism;
  }
}
