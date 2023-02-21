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

package com.bytedance.bitsail.connector.doris.error;

import com.bytedance.bitsail.common.exception.ErrorCode;

public enum DorisErrorCode implements ErrorCode {
  // Failed to init write proxy
  PROXY_INIT_FAILED("DorisWriter-01", "Failed to init write proxy"),
  REQUIRED_VALUE("DorisWriter-02", "You missed parameter which is required, please check your configuration."),
  LOAD_FAILED("DorisWriter-04", "Failed to load data into doris"),
  COMMIT_FAILED("DorisWriter-05", "Failed to commit data into doris"),
  GET_BACKEND_FAILED("DorisWriter-06", "Failed to get doris backend"),
  CONNECTED_FAILED("DorisWriter-07", "Failed to connected doris"),
  FE_NOT_AVAILABLE("DorisWriter-08", "Fe nodes not available"),
  PARSE_FAILED("DorisWriter-09", "Failed parse response"),
  ABORT_FAILED("DorisWriter-10", "Fail to abort transaction"),
  LABEL_ALREADY_EXIST("DorisWriter-11", "Label already exist"),
  ;

  private final String code;
  private final String description;

  DorisErrorCode(String code, String description) {
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
