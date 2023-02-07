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

package com.bytedance.bitsail.connector.assertion.sink.error;

import com.bytedance.bitsail.common.exception.ErrorCode;

public enum AssertErrorCode implements ErrorCode {

  RULE_CHECK_FAILED("Assert-01", "Rule check failed"),
  RULE_NOT_SUPPORT("Assert-02", "Rule not support yet"),
  DATATYPE_CAST_ERROR("Assert-03", "Data cast error");

  private final String code;

  private final String description;

  AssertErrorCode(String code, String description) {
    this.code = code;
    this.description = description;
  }

  public String getCode() {
    return code;
  }

  public String getDescription() {
    return description;
  }
}
