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

package com.bytedance.bitsail.connector.kudu.error;

import com.bytedance.bitsail.common.exception.ErrorCode;

public enum KuduErrorCode implements ErrorCode {

  REQUIRED_VALUE("Kudu-00", "The configuration file lack necessary options"),
  CONFIG_ERROR("Kudu-01", "The configuration has wrong option"),
  OPEN_TABLE_ERROR("Kudu-02", "Failed to open table"),
  UNSUPPORTED_OPERATION("Kudu-03", "Operation is not supported"),
  UNSUPPORTED_TYPE("Kudu-04", "Type is not supported"),
  ILLEGAL_VALUE("Kudu-05", "Value type is illegal"),
  SPLIT_ERROR("Kudu-06", "Something wrong with creating splits."),
  PREDICATE_ERROR("Kudu-07", "Something wrong with kudu predicates."),

  SCANNER_ERROR("Kudu-08", "Something wrong with kudu scanner.");

  private final String code;

  private final String describe;

  KuduErrorCode(String code, String describe) {
    this.code = code;
    this.describe = describe;
  }

  @Override
  public String getCode() {
    return code;
  }

  @Override
  public String getDescription() {
    return describe;
  }

  @Override
  public String toString() {
    return String.format("Code:[%s], Describe:[%s]", this.code,
        this.describe);
  }
}
