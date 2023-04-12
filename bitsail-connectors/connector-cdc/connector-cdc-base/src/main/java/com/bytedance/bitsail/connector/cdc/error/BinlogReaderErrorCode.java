/*
 * Copyright 2022-2023 Bytedance Ltd. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.bytedance.bitsail.connector.cdc.error;

import com.bytedance.bitsail.common.exception.ErrorCode;

public enum BinlogReaderErrorCode implements ErrorCode {

  REQUIRED_VALUE("Cdc-00", "The configuration file is lack of necessary options"),
  CONFIG_ERROR("Cdc-01", "The configuration has wrong option"),
  CONVERT_ERROR("Cdc-02", "Failed to convert mysql cdc result to row"),
  UNSUPPORTED_ERROR("Cdc-03", "Operation is not supported yet"),
  OFFSET_ERROR("Cdc-04", "Failed to load binlog offset"),

  SQL_ERROR("Cdc-05", "Failed to executing SQL");

  private final String code;

  private final String describe;

  BinlogReaderErrorCode(String code, String describe) {
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
