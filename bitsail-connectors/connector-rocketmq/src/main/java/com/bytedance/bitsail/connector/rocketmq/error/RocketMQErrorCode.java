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

package com.bytedance.bitsail.connector.rocketmq.error;

import com.bytedance.bitsail.common.exception.ErrorCode;

public enum RocketMQErrorCode implements ErrorCode {

  CONSUMER_CREATE_FAILED("RocketMQ-1", "RocketMQ Consumer create failed."),
  CONSUMER_FETCH_OFFSET_FAILED("RocketMQ-2", "RocketMQ Consumer fetch offset failed."),
  CONSUMER_SEEK_OFFSET_FAILED("RocketMQ-3", "RocketMQ Consumer seek offset failed."),
  REQUIRED_VALUE("RocketMQ-4", "You missed parameter which is required, please check your configuration."),
  UNSUPPORTED_FORMAT("RocketMQ-5", "Unsupported output format.");

  public String code;
  public String description;

  RocketMQErrorCode(String code, String description) {
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
