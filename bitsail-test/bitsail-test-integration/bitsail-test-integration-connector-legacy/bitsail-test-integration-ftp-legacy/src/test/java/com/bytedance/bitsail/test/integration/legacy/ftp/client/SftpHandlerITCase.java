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

package com.bytedance.bitsail.test.integration.legacy.ftp.client;

import com.bytedance.bitsail.connector.legacy.ftp.client.FtpHandlerFactory;
import com.bytedance.bitsail.connector.legacy.ftp.client.IFtpHandler;
import com.bytedance.bitsail.connector.legacy.ftp.common.FtpConfig;
import com.bytedance.bitsail.test.integration.legacy.ftp.container.FtpConfigUtils;
import com.bytedance.bitsail.test.integration.legacy.ftp.container.SftpDataSource;
import com.bytedance.bitsail.test.integration.legacy.ftp.container.constant.FtpTestConstants;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;

import java.io.IOException;
import java.util.Collections;

public class SftpHandlerITCase {

  private IFtpHandler sftpHandler;
  private static GenericContainer sftpServer;

  @Before
  public void setup() {
    sftpServer = SftpDataSource.create();
    sftpServer.start();

    FtpConfig config = FtpConfigUtils.initCommonConfig(sftpServer.getFirstMappedPort());
    sftpHandler = FtpHandlerFactory.createFtpHandler(FtpConfig.Protocol.SFTP);
    sftpHandler.loginFtpServer(config);
  }

  @After
  public void teardown() throws IOException {
    sftpHandler.logoutFtpServer();
    sftpServer.stop();
  }

  @Test
  public void testGetFiles() {
    Assert.assertEquals(3, sftpHandler.getFiles(FtpTestConstants.UPLOAD).size());
    Assert.assertEquals(Collections.singletonList(FtpTestConstants.UPLOAD + "test1.csv"),
        sftpHandler.getFiles(FtpTestConstants.UPLOAD + "test1.csv"));
    Assert.assertEquals(Collections.emptyList(), sftpHandler.getFiles(FtpTestConstants.UPLOAD + "badname"));
  }

  @Test
  public void testGetFilesSize() {
    Assert.assertEquals(104L, sftpHandler.getFilesSize(FtpTestConstants.UPLOAD));
    Assert.assertEquals(0L, sftpHandler.getFilesSize(FtpTestConstants.UPLOAD + FtpTestConstants.SUCCESS_TAG));
  }
}
