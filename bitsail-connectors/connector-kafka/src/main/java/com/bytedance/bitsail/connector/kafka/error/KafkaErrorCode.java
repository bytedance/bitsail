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

package com.bytedance.bitsail.connector.kafka.error;

import com.bytedance.bitsail.common.exception.ErrorCode;

public enum KafkaErrorCode implements ErrorCode {

  TOPIC_NOT_EXISTS("Kafka-0", "Kafka fetch partitions failed."),
  CONSUMER_CREATE_FAILED("Kafka-1", "Kafka AdminClient create failed."),
  CONSUMER_FETCH_OFFSET_FAILED("Kafka-2", "Kafka AdminClient fetch offset failed."),
  CONSUMER_SEEK_OFFSET_FAILED("Kafka-3", "Kafka AdminClient seek offset failed.");

  public final String code;
  public final String description;

  KafkaErrorCode(String code, String description) {
    this.code = code;
    this.description = description;
  }

  @Override
  public String getCode() {
    return code;
  }

  @Override
  public String getDescription() {
    return description;
  }
}
