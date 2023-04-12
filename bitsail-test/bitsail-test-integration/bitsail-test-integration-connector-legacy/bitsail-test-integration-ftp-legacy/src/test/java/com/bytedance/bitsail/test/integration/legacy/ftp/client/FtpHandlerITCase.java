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
import com.bytedance.bitsail.test.integration.legacy.ftp.container.FtpDataSource;
import com.bytedance.bitsail.test.integration.legacy.ftp.container.constant.FtpTestConstants;

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

    ftpHandler = FtpHandlerFactory.createFtpHandler(FtpConfig.Protocol.FTP);
    ftpHandler.loginFtpServer(config);
  }

  @After
  public void teardown() throws IOException {
    ftpHandler.logoutFtpServer();
    ftpServer.stop();
  }

  @Test
  public void getFilesTest() {
    Assert.assertEquals(3, ftpHandler.getFiles(FtpTestConstants.UPLOAD).size());
    Assert.assertEquals(Collections.singletonList(FtpTestConstants.UPLOAD + "test1.csv"),
        ftpHandler.getFiles(FtpTestConstants.UPLOAD + "test1.csv"));
    Assert.assertEquals(Collections.emptyList(),
        ftpHandler.getFiles(FtpTestConstants.UPLOAD + "badname"));
  }

  @Test
  public void getFilesSizeTest() {
    Assert.assertEquals(104L,
        ftpHandler.getFilesSize(FtpTestConstants.UPLOAD));
    Assert.assertEquals(0L,
        ftpHandler.getFilesSize(FtpTestConstants.UPLOAD + FtpTestConstants.SUCCESS_TAG));
  }

  @Test
  public void getFilesTestWithCharset() {
    Assert.assertEquals(3,
        ftpHandler.getFiles(FtpTestConstants.UPLOAD_CHARSET).size());
    Assert.assertEquals(Collections.singletonList(FtpTestConstants.UPLOAD_CHARSET + "test1.csv"),
        ftpHandler.getFiles(FtpTestConstants.UPLOAD_CHARSET + "test1.csv"));
    Assert.assertEquals(Collections.emptyList(),
        ftpHandler.getFiles(FtpTestConstants.UPLOAD_CHARSET + "badname"));
  }

  @Test
  public void getFilesSizeTestWithCharset() {
    Assert.assertEquals(106L,
        ftpHandler.getFilesSize(FtpTestConstants.UPLOAD_CHARSET));
    Assert.assertEquals(0L,
        ftpHandler.getFilesSize(FtpTestConstants.UPLOAD_CHARSET + FtpTestConstants.SUCCESS_TAG));
  }
}
