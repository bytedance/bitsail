/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bytedance.bitsail.util;

import com.bytedance.bitsail.connector.ftp.core.config.FtpConfig;

import org.mockftpserver.fake.FakeFtpServer;
import org.mockftpserver.fake.UserAccount;
import org.mockftpserver.fake.filesystem.DirectoryEntry;
import org.mockftpserver.fake.filesystem.FileEntry;
import org.mockftpserver.fake.filesystem.UnixFakeFileSystem;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;

import java.io.File;

import static com.bytedance.bitsail.util.Constant.CSV_SUCCESS_TAG;
import static com.bytedance.bitsail.util.Constant.CSV_UPLOAD1;
import static com.bytedance.bitsail.util.Constant.CSV_UPLOAD2;
import static com.bytedance.bitsail.util.Constant.DEFAULT_TIMEOUT;
import static com.bytedance.bitsail.util.Constant.FTP_PORT;
import static com.bytedance.bitsail.util.Constant.JSON_SUCCESS_TAG;
import static com.bytedance.bitsail.util.Constant.JSON_UPLOAD1;
import static com.bytedance.bitsail.util.Constant.JSON_UPLOAD2;
import static com.bytedance.bitsail.util.Constant.LOCALHOST;
import static com.bytedance.bitsail.util.Constant.PASSWORD;
import static com.bytedance.bitsail.util.Constant.SFTP_PORT;
import static com.bytedance.bitsail.util.Constant.USER;

public class SetupUtil {

  public FtpConfig initCommonConfig(int port) {
    FtpConfig config = new FtpConfig();
    config.setHost(LOCALHOST);
    config.setPort(port);
    config.setUsername(USER);
    config.setPassword(PASSWORD);
    config.setTimeout(DEFAULT_TIMEOUT);
    return config;
  }

  public FakeFtpServer getFTP() {
    FakeFtpServer fakeFtpServer = new FakeFtpServer();
    fakeFtpServer.addUserAccount(new UserAccount(USER, PASSWORD, CSV_UPLOAD1));

    UnixFakeFileSystem fakeFileSystem = new UnixFakeFileSystem();
    fakeFileSystem.add(new DirectoryEntry(CSV_UPLOAD1));
    fakeFileSystem.add(new FileEntry(CSV_UPLOAD1 + "test1.csv", "c0,c1,c2,c3,c4,c5,c6,c7,c8\n111,aaa,1.1,1.12345,true,2022-11-01,999999999,1669705101,123456.789"));
    fakeFileSystem.add(new FileEntry(CSV_UPLOAD1 + "test2.csv", "c0,c1,c2,c3,c4,c5,c6,c7\n-111,aaa,-1.1,-1.12345,false,2022-11-01,1111111111,1669605101,766355.001"));
    fakeFileSystem.add(new DirectoryEntry(CSV_UPLOAD2));
    fakeFileSystem.add(new FileEntry(CSV_UPLOAD2 + "test1.csv", "c0,c1,c2,c3,c4,c5,c6,c7,c8\n222,bbb,-9.1,1.12345,true,2022-11-01,999999999,1669705101,123456.789"));
    fakeFileSystem.add(new FileEntry(CSV_UPLOAD2 + "test2.csv", "c0,c1,c2,c3,c4,c5,c6,c7\n-222,ccc,1.1,-1.12345,false,2022-11-01,1111111111,1669605101,766355.001"));
    fakeFileSystem.add(new FileEntry(CSV_SUCCESS_TAG));
    fakeFileSystem.add(new DirectoryEntry(JSON_UPLOAD1));
    fakeFileSystem.add(
        new FileEntry(
            JSON_UPLOAD1 + "test1.json",
            "{\"c0\":222,\"c1\":\"bbb\",\"c2\":-9.1,\"c3\":1.12345,\"c4\":true,\"c5\":\"2022-11-01\",\"c6\":999999999,\"c7\":1669705101,\"c8\":123456.789}"));
    fakeFileSystem.add(
        new FileEntry(
            JSON_UPLOAD1 + "test2.json",
            "{\"c0\":111,\"c1\":\"bbb\",\"c2\":-9.1,\"c3\":1.12345,\"c4\":true,\"c5\":\"2022-11-01\",\"c6\":999999999,\"c7\":1669705101,\"c8\":123456.789}"));
    fakeFileSystem.add(new DirectoryEntry(JSON_UPLOAD2));
    fakeFileSystem.add(
        new FileEntry(
            JSON_UPLOAD2 + "test1.json",
            "{\"c0\":-111,\"c1\":\"mmm\",\"c2\":-9.1,\"c3\":1.12345,\"c4\":true,\"c5\":\"2022-11-01\",\"c6\":999999999,\"c7\":1669705101,\"c8\":123456.789}"));
    fakeFileSystem.add(
        new FileEntry(
            JSON_UPLOAD2 + "test2.json",
            "{\"c0\":-222,\"c1\":\"ccc\",\"c2\":-9.1,\"c3\":1.12345,\"c4\":true,\"c5\":\"2022-11-01\",\"c6\":1111111111,\"c7\":1669605101,\"c8\":766355.001}"));
    fakeFileSystem.add(new FileEntry(JSON_SUCCESS_TAG));
    fakeFtpServer.setFileSystem(fakeFileSystem);
    fakeFtpServer.setServerControlPort(FTP_PORT);
    return fakeFtpServer;
  }

  public GenericContainer getSFTP() {
    File testDir = new File("src/test/resources/data");

    return new GenericContainer("atmoz/sftp")
        .withExposedPorts(SFTP_PORT)
        .withFileSystemBind(testDir.getAbsolutePath(), "/home/" + USER + "/data/", BindMode.READ_ONLY)
        .withCommand(USER + ":" + PASSWORD + ":1001");
  }
}
