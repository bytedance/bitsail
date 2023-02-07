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

package com.bytedance.bitsail.connector.ftp.core.config;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.connector.ftp.error.FtpErrorCode;
import com.bytedance.bitsail.connector.ftp.option.FtpReaderOptions;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Setter
@Getter
public class FtpConfig implements Serializable {

  private String username;

  private String password;

  private String[] paths;

  private String host;

  private Integer port;

  private Boolean skipFirstLine;

  private int timeout;

  private Protocol protocol;

  private ConnectPattern connectPattern;
  private ContentType contentType;

  private int maxRetryTime;

  private int successFileRetryIntervalMs;

  private Boolean enableSuccessFileCheck;
  private String successFilePath;

  public enum Protocol {
    FTP, SFTP
  }

  // In ftp mode, connect pattern can be PASV or PORT
  // In sftp mode, connect pattern is NULL
  public enum ConnectPattern {
    PASV, PORT, NULL
  }

  public enum ContentType {
    JSON,
    CSV
  }

  public FtpConfig() {}

  public FtpConfig(BitSailConfiguration jobConf) {
    this.password = jobConf.getNecessaryOption(FtpReaderOptions.PASSWORD, FtpErrorCode.REQUIRED_VALUE);
    this.port = jobConf.get(FtpReaderOptions.PORT);
    this.host = jobConf.getNecessaryOption(FtpReaderOptions.HOST, FtpErrorCode.REQUIRED_VALUE);
    this.paths = jobConf.getNecessaryOption(FtpReaderOptions.PATH_LIST, FtpErrorCode.REQUIRED_VALUE).split(",");
    this.username = jobConf.getNecessaryOption(FtpReaderOptions.USER, FtpErrorCode.REQUIRED_VALUE);
    this.password = jobConf.getNecessaryOption(FtpReaderOptions.PASSWORD, FtpErrorCode.REQUIRED_VALUE);
    this.timeout = jobConf.get(FtpReaderOptions.TIME_OUT);
    this.maxRetryTime = jobConf.get(FtpReaderOptions.MAX_RETRY_TIME);
    this.successFileRetryIntervalMs = jobConf.get(FtpReaderOptions.RETRY_INTERVAL_MS);
    this.skipFirstLine = jobConf.get(FtpReaderOptions.SKIP_FIRST_LINE);
    this.protocol = FtpConfig.Protocol.valueOf(jobConf.getNecessaryOption(FtpReaderOptions.PROTOCOL, FtpErrorCode.REQUIRED_VALUE).toUpperCase());
    this.contentType = FtpConfig.ContentType.valueOf(jobConf.getNecessaryOption(FtpReaderOptions.CONTENT_TYPE, FtpErrorCode.UNSUPPORTED_TYPE).toUpperCase());
    this.connectPattern = FtpConfig.ConnectPattern.valueOf(jobConf.get(FtpReaderOptions.CONNECT_PATTERN).toUpperCase());
    this.enableSuccessFileCheck = jobConf.get(FtpReaderOptions.ENABLE_SUCCESS_FILE_CHECK);
    if (this.enableSuccessFileCheck) {
      this.successFilePath = jobConf.getNecessaryOption(FtpReaderOptions.SUCCESS_FILE_PATH, FtpErrorCode.SUCCESS_FILEPATH_REQUIRED);
    }
  }
}
