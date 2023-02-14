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

package com.bytedance.bitsail.test.e2e.datasource;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.option.ReaderOptions;
import com.bytedance.bitsail.connector.ftp.core.config.FtpConfig;
import com.bytedance.bitsail.connector.ftp.option.FtpReaderOptions;
import com.bytedance.bitsail.connector.ftp.source.FtpSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;

public class SftpDataSource extends AbstractDataSource {
  private static final Logger LOG = LoggerFactory.getLogger(SftpDataSource.class);

  private static final String SFTP_VERSION = "atmoz/sftp";
  private static final int SFTP_PORT = 22;

  /**
   * Auth info.
   */
  private static final String USER_NAME = "user";
  private static final String PASSWORD = "password";

  /**
   * Data paths.
   */
  private static final String CONTAINER_PATH = "/home/" + USER_NAME + "/data/";
  private static final String[] DATA_PATHS = {
      "/data/csv/upload1/",
      "/data/csv/upload2/"
  };

  /**
   * Connection info.
   */
  private String host;
  private int port;

  private GenericContainer<?> sftpServer;

  @Override
  public String getContainerName() {
    return "data-source-sftp";
  }

  @Override
  public void initNetwork(Network executorNetwork) {
    this.network = executorNetwork;
  }

  @Override
  public void configure(BitSailConfiguration dataSourceConf) {

  }

  @Override
  public boolean accept(BitSailConfiguration jobConf, Role role) {
    String readerClass = jobConf.get(ReaderOptions.READER_CLASS);
    FtpConfig.Protocol protocol = FtpConfig.Protocol.valueOf(
        jobConf.get(FtpReaderOptions.PROTOCOL).toUpperCase());

    return role == Role.SOURCE
        && FtpConfig.Protocol.SFTP == protocol
        && FtpSource.class.getName().equals(readerClass);
  }

  @Override
  public void modifyJobConf(BitSailConfiguration jobConf) {
    jobConf.set(FtpReaderOptions.HOST, host);
    jobConf.set(FtpReaderOptions.PORT, port);
    jobConf.set(FtpReaderOptions.USER, USER_NAME);
    jobConf.set(FtpReaderOptions.PASSWORD, PASSWORD);
    jobConf.setIfAbsent(FtpReaderOptions.PATH_LIST, String.join(",", DATA_PATHS));
    jobConf.setIfAbsent(FtpReaderOptions.CONTENT_TYPE, "csv");
    jobConf.setIfAbsent(FtpReaderOptions.ENABLE_SUCCESS_FILE_CHECK, false);
    jobConf.setIfAbsent(FtpReaderOptions.SKIP_FIRST_LINE, true);
  }

  @Override
  public void start() {
    this.host = getContainerName() + "-" + role;
    this.port = SFTP_PORT;

    sftpServer = new GenericContainer<>(DockerImageName.parse(SFTP_VERSION))
        .withNetwork(network)
        .withNetworkAliases(host)
        .withExposedPorts(port)
        .withCommand(String.format("%s:%s:1001", USER_NAME, PASSWORD));
    sftpServer.addFileSystemBind(
        MountableFile.forClasspathResource("/data").getFilesystemPath(),
        CONTAINER_PATH,
        BindMode.READ_ONLY
    );

    sftpServer.start();
    LOG.info("Sftp container starts! Host is: [{}], port is: [{}].", host, port);
  }

  @Override
  public void close() throws IOException {
    sftpServer.close();
    LOG.info("Sftp container closed.");

    super.close();
  }
}
