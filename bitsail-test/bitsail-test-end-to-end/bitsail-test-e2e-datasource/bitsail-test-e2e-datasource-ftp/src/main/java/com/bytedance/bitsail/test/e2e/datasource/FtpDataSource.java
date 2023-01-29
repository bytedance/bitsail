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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.MountableFile;

import java.io.File;
import java.io.IOException;
import java.util.Objects;

public class FtpDataSource extends AbstractDataSource {
  private static final Logger LOG = LoggerFactory.getLogger(FtpDataSource.class);

  private static final String FTP_DOCKER_IMAGE = "delfer/alpine-ftp-server:latest";
  private static final int PASSIVE_MODE_PORT = 21000;

  private FixedHostPortGenericContainer<?> ftpServer;

  private String internalHost;
  private int internalPort;

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
    internalHost = getContainerName() + "-" + role;
    internalPort = Constants.FTP_PORT;
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

    jobConf.set(FtpReaderOptions.HOST, internalHost);
    jobConf.set(FtpReaderOptions.PORT, internalPort);

    jobConf.set(FtpReaderOptions.USER, Constants.USER);
    jobConf.set(FtpReaderOptions.PASSWORD, Constants.PASSWORD);
  }

  @Override
  public void start() {
    ftpServer = new FixedHostPortGenericContainer<>(FTP_DOCKER_IMAGE)
        .withNetwork(network)
        .withNetworkAliases(internalHost)
        .withFixedExposedPort(PASSIVE_MODE_PORT, PASSIVE_MODE_PORT)
        .withFixedExposedPort(internalPort, internalPort)
        .withExposedPorts(internalPort)
        .withEnv("MIN_PORT", String.valueOf(PASSIVE_MODE_PORT))
        .withEnv("MAX_PORT", String.valueOf(PASSIVE_MODE_PORT))
        .withEnv("USERS", String.format("%s|%s|/", Constants.USER, Constants.PASSWORD));

    File dataResources = new File(Objects.requireNonNull(
        FtpDataSource.class.getClassLoader().getResource("data")).getPath());
    ftpServer.withCopyFileToContainer(
        MountableFile.forHostPath(dataResources.getAbsolutePath()),
        "/data/"
    );

    ftpServer.start();

    LOG.info("Ftp container starts! Host is: [{}], port is: [{}].", internalHost, internalPort);
  }

  @Override
  public void fillData() {
    //
  }

  @Override
  public void close() throws IOException {
    if (ftpServer != null) {
      ftpServer.stop();
      ftpServer.close();
      ftpServer = null;
    }
    super.close();
  }
}
