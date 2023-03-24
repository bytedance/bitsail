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

import org.mockftpserver.fake.FakeFtpServer;
import org.mockftpserver.fake.UserAccount;
import org.mockftpserver.fake.filesystem.DirectoryEntry;
import org.mockftpserver.fake.filesystem.FileEntry;
import org.mockftpserver.fake.filesystem.UnixFakeFileSystem;

public class FtpDataSource {

  public static FakeFtpServer create() {
    FakeFtpServer fakeFtpServer = new FakeFtpServer();
    fakeFtpServer.addUserAccount(
        new UserAccount(FtpTestConstants.USER, FtpTestConstants.PASSWORD, FtpTestConstants.CSV_UPLOAD1));

    UnixFakeFileSystem fakeFileSystem = new UnixFakeFileSystem();
    fakeFileSystem.add(new DirectoryEntry(FtpTestConstants.CSV_UPLOAD1));
    fakeFileSystem.add(
        new FileEntry(
            FtpTestConstants.CSV_UPLOAD1 + "test1.csv",
            "c0,c1,c2,c3,c4,c5,c6,c7,c8\n111,aaa,1.1,1.12345,true,2022-11-01,999999999,1669705101,123456.789\n"
                + "222,bbb,1.1,1.45678,true,2022-10-01,999999999,1669705101,345678.123\n"
                + "333,ccc,0.1,1.12345,false,2022-01-01,88888888,1669715106,123456.789\n"
                + "444,ccc,0.1,1.12345,false,2022-01-01,88888888,1669715106,123456.789\n"
                + "555,ccc,0.1,1.12345,false,2022-01-01,11111111,1669715106,343.345"));
    fakeFileSystem.add(
        new FileEntry(
            FtpTestConstants.CSV_UPLOAD1 + "test2.csv",
            "c0,c1,c2,c3,c4,c5,c6,c7\n-111,aaa,-1.1,-1.12345,false,2022-11-01,1111111111,1669605101,766355.001\n"
                + "-999,eee,-1.1,-1.12345,false,2022-11-01,1111111111,1669605101,766355.001\n"
                + "-222,fff,-1.1,-1.12345,false,2022-11-01,1111111111,1669605202,766355.001\n"
                + "-555,bbd,100.1,-1.12345,false,2022-11-01,3253636344,1669605101,766355.001\n"
                + "-666,ewe,9.1,-1.12345,false,2022-11-01,1111111111,1669604113,353436.123"));
    fakeFileSystem.add(new DirectoryEntry(FtpTestConstants.CSV_UPLOAD2));
    fakeFileSystem.add(
        new FileEntry(
            FtpTestConstants.CSV_UPLOAD2 + "test1.csv",
            "c0,c1,c2,c3,c4,c5,c6,c7,c8\n222,bbb,-9.1,1.12345,true,2022-11-01,999999999,1669705101,123456.789\n"
                + "111,bbb,-9.1,1.12345,true,2022-11-01,999999999,1669705101,123456.789\n"
                + "999,bbb,78.1,1.12345,true,2022-05-01,999999999,1669705101,43536.5\n"
                + "666,bbb,32.3,3.456,true,2022-11-01,35636346,1669403121,3463.3\n"
                + "444,bbb,-9.1,1.12345,true,2022-03-31,4364646,1669705101,757.51"));
    fakeFileSystem.add(
        new FileEntry(
            FtpTestConstants.CSV_UPLOAD2 + "test2.csv",
            "c0,c1,c2,c3,c4,c5,c6,c7\n-222,ccc,1.1,-1.12345,false,2022-11-01,1111111111,1669605101,2313.001\n"
                + "789,ccc,1.1,-1.12345,false,2022-08-27,1111111111,1669605101,43536.001\n"
                + "456,ccc,5.6,-1.12345,false,2022-11-01,1111111111,1669605101,76969.657\n"
                + "4636,ccc,8.8,-1.5345,false,2022-06-21,1111111111,1669343101,766355.001\n"
                + "-398,ccc,1.1,-1.12345,false,2022-11-01,1111111111,1669605101,47547.345"));
    fakeFileSystem.add(new FileEntry(FtpTestConstants.CSV_SUCCESS_TAG));
    fakeFileSystem.add(new DirectoryEntry(FtpTestConstants.JSON_UPLOAD1));
    fakeFileSystem.add(
        new FileEntry(
            FtpTestConstants.JSON_UPLOAD1 + "test1.json",
            "{\"c0\":222,\"c1\":\"bbb\",\"c2\":-9.1,\"c3\":1.12345,\"c4\":true,\"c5\":\"2022-11-01\",\"c6\":999999999,\"c7\":1669705101,\"c8\":123456.789}"));
    fakeFileSystem.add(
        new FileEntry(
            FtpTestConstants.JSON_UPLOAD1 + "test2.json",
            "{\"c0\":111,\"c1\":\"bbb\",\"c2\":-9.1,\"c3\":1.12345,\"c4\":true,\"c5\":\"2022-11-01\",\"c6\":999999999,\"c7\":1669705101,\"c8\":123456.789}"));
    fakeFileSystem.add(new DirectoryEntry(FtpTestConstants.JSON_UPLOAD2));
    fakeFileSystem.add(
        new FileEntry(
            FtpTestConstants.JSON_UPLOAD2 + "test1.json",
            "{\"c0\":-111,\"c1\":\"mmm\",\"c2\":-9.1,\"c3\":1.12345,\"c4\":true,\"c5\":\"2022-11-01\",\"c6\":999999999,\"c7\":1669705101,\"c8\":123456.789}"));
    fakeFileSystem.add(
        new FileEntry(
            FtpTestConstants.JSON_UPLOAD2 + "test2.json",
            "{\"c0\":-222,\"c1\":\"ccc\",\"c2\":-9.1,\"c3\":1.12345,\"c4\":true,\"c5\":\"2022-11-01\",\"c6\":1111111111,\"c7\":1669605101,\"c8\":766355.001}"));
    fakeFileSystem.add(new FileEntry(FtpTestConstants.JSON_SUCCESS_TAG));
    fakeFtpServer.setFileSystem(fakeFileSystem);
    fakeFtpServer.setServerControlPort(FtpTestConstants.FTP_PORT);
    return fakeFtpServer;
  }
}
