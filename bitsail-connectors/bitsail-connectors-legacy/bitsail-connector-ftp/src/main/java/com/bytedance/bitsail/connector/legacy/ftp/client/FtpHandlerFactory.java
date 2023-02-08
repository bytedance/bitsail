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

package com.bytedance.bitsail.connector.legacy.ftp.client;

import com.bytedance.bitsail.connector.legacy.ftp.common.FtpConfig;

public class FtpHandlerFactory {
  public static IFtpHandler createFtpHandler(FtpConfig.Protocol protocol) {
    IFtpHandler ftpHandler;
    if (protocol == FtpConfig.Protocol.SFTP) {
      ftpHandler = new SftpHandler();
    } else {
      ftpHandler = new FtpHandler();
    }
    return ftpHandler;
  }
}
