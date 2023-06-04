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

package com.bytedance.bitsail.connector.kafka.source.split;

import org.apache.kafka.common.TopicPartition;

import java.io.Serializable;
import java.util.Map;

public class KafkaState implements Serializable {

  private final Map<TopicPartition, String> assignedWithSplitsIds;

  public KafkaState(Map<TopicPartition, String> assignedWithSplitsIds) {
    this.assignedWithSplitsIds = assignedWithSplitsIds;
  }

  public Map<TopicPartition, String> getAssignedWithSplitsIds() {
    return this.assignedWithSplitsIds;
  }
}
