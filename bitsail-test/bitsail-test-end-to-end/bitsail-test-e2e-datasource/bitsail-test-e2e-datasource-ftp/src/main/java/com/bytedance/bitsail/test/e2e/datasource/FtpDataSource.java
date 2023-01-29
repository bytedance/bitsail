/*
 * Copyright 2022 Bytedance Ltd. and/or its affiliates.
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

package com.bytedance.bitsail.test.e2e.datasource;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.option.ReaderOptions;
import com.bytedance.bitsail.connector.ftp.core.config.FtpConfig;
import com.bytedance.bitsail.connector.ftp.option.FtpReaderOptions;
import com.bytedance.bitsail.connector.ftp.source.FtpSource;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.Network;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Objects;

public class FtpDataSource extends AbstractDataSource {
  private static final Logger LOG = LoggerFactory.getLogger(FtpDataSource.class);

  private static final String FTP_DOCKER_IMAGE = "delfer/alpine-ftp-server:latest";
  private static final int PASSIVE_MODE_PORT = 21000;

  private FixedHostPortGenericContainer<?> ftpServer;

  @Override
  public String getContainerName() {
    return "data-source-ftp";
  }

  @Override
  public void initNetwork(Network executorNetwork) {
    this.network = executorNetwork;
  }

  @Override
  public void configure(BitSailConfiguration dataSourceConf) {
    //
  }

  @Override
  public boolean accept(BitSailConfiguration jobConf, Role role) {
    String readerClass = jobConf.get(ReaderOptions.READER_CLASS);
    FtpConfig.Protocol protocol = FtpConfig.Protocol.valueOf(
        jobConf.get(FtpReaderOptions.PROTOCOL).toUpperCase());

    return role == Role.SOURCE
        && FtpSource.class.getName().equals(readerClass)
        && protocol == FtpConfig.Protocol.FTP;
  }

  @Override
  public void modifyJobConf(BitSailConfiguration jobConf) {
    jobConf.set(FtpReaderOptions.PROTOCOL, FtpConfig.Protocol.FTP.name());

    jobConf.set(FtpReaderOptions.HOST, Constants.LOCALHOST);
    jobConf.set(FtpReaderOptions.PORT, Constants.FTP_PORT);

    jobConf.set(FtpReaderOptions.USER, Constants.USER);
    jobConf.set(FtpReaderOptions.PASSWORD, Constants.PASSWORD);
  }

  @Override
  public void start() {
    File dataResources = new File(Objects.requireNonNull(
        FtpDataSource.class.getClassLoader().getResource("data")).getPath());

    ftpServer = new FixedHostPortGenericContainer<>(FTP_DOCKER_IMAGE)
        .withFixedExposedPort(PASSIVE_MODE_PORT, PASSIVE_MODE_PORT)
        .withEnv("MIN_PORT", String.valueOf(PASSIVE_MODE_PORT))
        .withEnv("MAX_PORT", String.valueOf(PASSIVE_MODE_PORT))
        .withExposedPorts(Constants.FTP_PORT)
        .withEnv("USERS", String.format("%s|%s", Constants.USER, Constants.PASSWORD))
        .withFileSystemBind(dataResources.getAbsolutePath(),
            "/home/" + Constants.USER + "/data/", BindMode.READ_ONLY);
    ftpServer.start();
    LOG.info("Ftp server started.");
  }

  @Override
  public void fillData() {
    FTPClient ftpClient = new FTPClient();
    try {
      FTPFile[] files = ftpClient.listFiles("/home/user/data/");
      for (FTPFile file : files) {
        System.out.println(file);
      }
    } catch (Exception e) {
      //
    }
  }

  @Override
  public void close() throws IOException {
    super.close();
  }

  /**
   * Reader resource file as string.
   */
  private static String readResource(String path) {
    if (path.startsWith("/")) {
      path = path.substring(1);
    }
    try {
      URL url = Thread.currentThread()
          .getContextClassLoader()
          .getResource(path);
      return new String(Files.readAllBytes(Paths.get(url.getPath())));
    } catch (Exception e) {
      throw new RuntimeException("Failed to read test csv data from " + path, e);
    }
  }
}
