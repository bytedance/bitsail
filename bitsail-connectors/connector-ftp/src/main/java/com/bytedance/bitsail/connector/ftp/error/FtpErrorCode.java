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

package com.bytedance.bitsail.connector.ftp.error;

import com.bytedance.bitsail.common.exception.ErrorCode;

public enum FtpErrorCode implements ErrorCode {
  REQUIRED_VALUE("Ftp-01", "You missed parameter which is required, please check your configuration."),
  SUCCESS_FILE_NOT_EXIST("Ftp-02", "success file isn't ready, please check success file is exist."),
  CONNECTION_ERROR("Ftp-03", "Error occured while getting ftp server connection."),
  CONFIG_ERROR("Ftp-04", "Config parameter is error."),
  FILEPATH_NOT_EXIST("Ftp-05", "File paths isn't existed, please check filePath is correct."),
  SPLIT_ERROR("Ftp-06", "Something wrong with creating splits."),
  UNSUPPORTED_TYPE("Ftp-07", "Content Type is not supported"),
  SUCCESS_FILEPATH_REQUIRED("Ftp-08", "when enable_success_file_check is set to true, the success_file_path is required."),
  CONTENT_TYPE_NOT_SUPPORTED("Ftp-09", "Content type only supports JSON and CSV.");

  private final String code;
  private final String description;

  FtpErrorCode(String code, String description) {
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
