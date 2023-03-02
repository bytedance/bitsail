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

package com.bytedance.bitsail.test.integration.legacy.ftp.container;

import com.bytedance.bitsail.test.integration.legacy.ftp.container.constant.FtpTestConstants;

import org.mockftpserver.fake.FakeFtpServer;
import org.mockftpserver.fake.UserAccount;
import org.mockftpserver.fake.filesystem.DirectoryEntry;
import org.mockftpserver.fake.filesystem.FileEntry;
import org.mockftpserver.fake.filesystem.UnixFakeFileSystem;

import java.io.UnsupportedEncodingException;

public class FtpDataSource {

  public static FakeFtpServer create() {
    FakeFtpServer fakeFtpServer = new FakeFtpServer();
    fakeFtpServer.addUserAccount(new UserAccount(
        FtpTestConstants.USER, FtpTestConstants.PASSWORD, FtpTestConstants.UPLOAD));

    UnixFakeFileSystem fakeFileSystem = new UnixFakeFileSystem();
    fakeFileSystem.add(new DirectoryEntry(FtpTestConstants.UPLOAD));
    fakeFileSystem.add(new FileEntry(FtpTestConstants.UPLOAD + "test1.csv",
        "c0,c1,c2,c3,c4\n111,aaa,1.1,1.12345,true,2022-11-01"));
    fakeFileSystem.add(new FileEntry(FtpTestConstants.UPLOAD + "test2.csv",
        "c0,c1,c2,c3,c4\n-111,aaa,-1.1,-1.12345,false,2022-11-01"));
    fakeFileSystem.add(new FileEntry(FtpTestConstants.UPLOAD + FtpTestConstants.SUCCESS_TAG));

    fakeFileSystem.add(new DirectoryEntry(FtpTestConstants.UPLOAD_CHARSET));
    FileEntry f1 = new FileEntry(FtpTestConstants.UPLOAD_CHARSET + "test1.csv");
    FileEntry f2 = new FileEntry(FtpTestConstants.UPLOAD_CHARSET + "test2.csv");
    try {
      f1.setContents("c0,c1,c2,c3,c4\n111," +
          FtpTestConstants.CHINESE_STR + ",1.1,1.12345,true,2022-11-01", "gbk");
      f2.setContents("c0,c1,c2,c3,c4\n-111," +
          FtpTestConstants.CHINESE_STR + ",-1.1,-1.12345,false,2022-11-01", "gbk");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
    fakeFileSystem.add(f1);
    fakeFileSystem.add(f2);
    fakeFileSystem.add(new FileEntry(FtpTestConstants.UPLOAD_CHARSET + FtpTestConstants.SUCCESS_TAG));

    fakeFtpServer.setFileSystem(fakeFileSystem);
    fakeFtpServer.setServerControlPort(FtpTestConstants.FTP_PORT);
    return fakeFtpServer;
  }
}
