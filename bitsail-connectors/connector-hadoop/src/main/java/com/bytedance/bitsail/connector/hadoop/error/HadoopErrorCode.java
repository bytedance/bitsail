/*
 * Copyright 2022 Bytedance Ltd. and/or its affiliates.
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

package com.bytedance.bitsail.connector.hadoop.error;

import com.bytedance.bitsail.common.exception.ErrorCode;

public enum HadoopErrorCode implements ErrorCode {

  REQUIRED_VALUE("Hadoop-01", "You missed parameter which is required, please check your configuration."),
  UNSUPPORTED_ENCODING("Hadoop-02", "Unsupported Encoding."),
  UNSUPPORTED_COLUMN_TYPE("Hadoop-03", "Unsupported column type."),
  HDFS_IO("Hadoop-04", "IO Exception.");

  private final String code;
  private final String description;

  HadoopErrorCode(String code, String description) {
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
