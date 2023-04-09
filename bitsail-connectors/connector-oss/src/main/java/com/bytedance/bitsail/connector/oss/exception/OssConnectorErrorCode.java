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

package com.bytedance.bitsail.connector.oss.exception;

import com.bytedance.bitsail.common.exception.ErrorCode;

public enum OssConnectorErrorCode implements ErrorCode {
  REQUIRED_VALUE("Oss-01", "You missed parameter which is required, please check your configuration."),
  CONFIG_ERROR("Oss-02", "Config parameter is error."),
  UNSUPPORTED_TYPE("Oss-07", "Content Type is not supported"),
  FILE_OPERATION_FAILED("Oss-04", "File Operation Failed"),
  SPLIT_ERROR("Oss-05", "Something wrong with creating splits.");
  private final String code;
  private final String description;

  OssConnectorErrorCode(String code, String description) {
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
    return super.toString();
  }
}