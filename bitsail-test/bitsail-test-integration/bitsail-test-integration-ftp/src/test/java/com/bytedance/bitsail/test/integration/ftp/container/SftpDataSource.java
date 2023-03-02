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

package com.bytedance.bitsail.test.integration.ftp.container;

import com.bytedance.bitsail.test.integration.ftp.container.constant.FtpTestConstants;

import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;

import java.io.File;

public class SftpDataSource {

  public static GenericContainer create() {
    File testDir = new File("src/test/resources/data");

    return new GenericContainer("atmoz/sftp")
        .withExposedPorts(FtpTestConstants.SFTP_PORT)
        .withFileSystemBind(testDir.getAbsolutePath(),
            "/home/" + FtpTestConstants.USER + "/data/",
            BindMode.READ_ONLY)
        .withCommand(FtpTestConstants.USER + ":" + FtpTestConstants.PASSWORD + ":1001");
  }
}
