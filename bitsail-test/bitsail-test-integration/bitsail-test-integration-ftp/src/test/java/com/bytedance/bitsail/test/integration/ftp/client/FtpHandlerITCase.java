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

package com.bytedance.bitsail.test.integration.ftp.client;

import com.bytedance.bitsail.connector.ftp.core.client.FtpHandlerFactory;
import com.bytedance.bitsail.connector.ftp.core.client.IFtpHandler;
import com.bytedance.bitsail.connector.ftp.core.config.FtpConfig;
import com.bytedance.bitsail.test.integration.ftp.container.FtpConfigUtils;
import com.bytedance.bitsail.test.integration.ftp.container.FtpDataSource;
import com.bytedance.bitsail.test.integration.ftp.container.constant.FtpTestConstants;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockftpserver.fake.FakeFtpServer;

import java.io.IOException;
import java.util.Collections;

public class FtpHandlerITCase {

  private FakeFtpServer ftpServer;
  private IFtpHandler ftpHandler;

  @Before
  public void setup() {
    ftpServer = FtpDataSource.create();
    ftpServer.start();

    FtpConfig config = FtpConfigUtils.initCommonConfig(ftpServer.getServerControlPort());
    config.setConnectPattern(FtpConfig.ConnectPattern.valueOf("PORT"));

    ftpHandler = FtpHandlerFactory.createFtpHandler(config);
    ftpHandler.loginFtpServer();
  }

  @After
  public void teardown() throws IOException {
    ftpHandler.logoutFtpServer();
    ftpServer.stop();
  }

  @Test
  public void getFilesTest() {
    Assert.assertEquals(2, ftpHandler.getFiles(FtpTestConstants.CSV_UPLOAD1).size());
    Assert.assertEquals(2, ftpHandler.getFiles(FtpTestConstants.CSV_UPLOAD2).size());
    Assert.assertEquals(Collections.singletonList(FtpTestConstants.CSV_UPLOAD1 + "test1.csv"),
        ftpHandler.getFiles(FtpTestConstants.CSV_UPLOAD1 + "test1.csv"));
    Assert.assertEquals(Collections.emptyList(),
        ftpHandler.getFiles(FtpTestConstants.CSV_UPLOAD1 + "badname"));
  }

  @Test
  public void getFilesSizeTest() {
    Assert.assertEquals(751L, ftpHandler.getFilesSize(FtpTestConstants.CSV_UPLOAD1));
    Assert.assertEquals(0L, ftpHandler.getFilesSize(FtpTestConstants.CSV_SUCCESS_TAG));
  }
}
