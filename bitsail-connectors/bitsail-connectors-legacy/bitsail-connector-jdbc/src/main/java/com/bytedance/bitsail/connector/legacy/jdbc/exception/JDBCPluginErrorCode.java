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

package com.bytedance.bitsail.connector.legacy.jdbc.exception;

import com.bytedance.bitsail.common.exception.ErrorCode;

/**
 * @desc:
 */
public enum JDBCPluginErrorCode implements ErrorCode {

  REQUIRED_VALUE("JDBCPlugin-01", "You missed parameter which is required, please check your configuration."),
  ILLEGAL_VALUE("JDBCPlugin-02", "The parameter value in your configuration is not valid."),
  INTERNAL_ERROR("JDBCPlugin-03", "Internal error occurred, which is usually a BitSail bug."),
  CONNECTION_ERROR("JDBCPlugin-04", "Error occurred while getting database connection."),
  DYNAMIC_EXTRA_PARTITION_ERROR("JDBCPlugin-05", "Dynamic extra partition error occur, value can't be null in dynamic partitions");

  private final String code;
  private final String description;

  JDBCPluginErrorCode(String code, String description) {
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
    return String.format("Code:[%s], Description:[%s].", this.code,
        this.description);
  }
}
