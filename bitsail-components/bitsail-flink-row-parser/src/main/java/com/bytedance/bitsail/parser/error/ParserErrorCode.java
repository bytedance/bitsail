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

package com.bytedance.bitsail.parser.error;

import com.bytedance.bitsail.common.exception.ErrorCode;

public enum ParserErrorCode implements ErrorCode {
  REQUIRED_VALUE("BitSailParser-01", "You missed parameter which is required, please check your configuration."),
  UNSUPPORTED_ENCODING("BitSailParser-04", "Encoding type not support in "),
  TOO_MANY_ELEMENTS_IN_ONE_FIELD("BitSailParser-05", "There are too many elements in one field, please check your origin data"),
  ILLEGAL_TEXT("BitSailParser-07", "It is an illegal text content"),
  ILLEGAL_JSON("BitSailParser-08", "It is an illegal json content"),
  UNSUPPORTED_COLUMN_TYPE("BitSailParser-09", "Unsupported column type.");
  private final String code;
  private final String description;

  ParserErrorCode(String code, String description) {
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
