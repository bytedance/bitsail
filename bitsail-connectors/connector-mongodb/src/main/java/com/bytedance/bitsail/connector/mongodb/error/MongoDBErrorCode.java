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

package com.bytedance.bitsail.connector.mongodb.error;

import com.bytedance.bitsail.common.exception.ErrorCode;

/**
 * Error code for MongoDB
 */
public enum MongoDBErrorCode implements ErrorCode {
  /**
   * Missed parameter(s)
   */
  REQUIRED_VALUE("MongoDB-01", "You missed parameter which is required, please check your configuration."),
  /**
   * Unsupported data type
   */
  UNSUPPORTED_TYPE("MongoDB-02", "The type is not supported by MongoDB connector."),
  /**
   * Convert to row error
   */
  CONVERT_ERROR("MongoDB-02", "Failed to convert MongoDB result to BitSail row");

  private final String code;
  private final String description;

  MongoDBErrorCode(String code, String description) {
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

  @Override
  public String toString() {
    return String.format("Code:[%s], Description:[%s].", this.code, this.description);
  }
}
