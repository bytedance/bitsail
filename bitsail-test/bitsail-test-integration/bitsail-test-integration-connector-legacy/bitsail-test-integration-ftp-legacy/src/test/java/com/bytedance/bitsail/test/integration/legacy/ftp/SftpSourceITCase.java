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

package com.bytedance.bitsail.test.integration.legacy.ftp;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.connector.legacy.ftp.common.FtpConfig;
import com.bytedance.bitsail.connector.legacy.ftp.option.FtpReaderOptions;
import com.bytedance.bitsail.test.integration.AbstractIntegrationTest;
import com.bytedance.bitsail.test.integration.legacy.ftp.container.SftpDataSource;
import com.bytedance.bitsail.test.integration.utils.JobConfUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;

public class SftpSourceITCase extends AbstractIntegrationTest {

  private GenericContainer sftpServer;

  @Before
  public void setup() {
    sftpServer = SftpDataSource.create();
    sftpServer.start();
  }

  @After
  public void teardown() {
    sftpServer.stop();
  }

  @Test
  public void testSftpSource() throws Exception {
    BitSailConfiguration globalConfiguration = JobConfUtils.fromClasspath("scripts/ftp_to_print.json");
    globalConfiguration.set(FtpReaderOptions.PROTOCOL, FtpConfig.Protocol.SFTP.name());
    globalConfiguration.set(FtpReaderOptions.PORT, sftpServer.getFirstMappedPort());
    submitJob(globalConfiguration);
  }
}
