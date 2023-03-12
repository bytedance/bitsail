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

package com.bytedance.bitsail.test.integration.legacy.oracle;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.connector.legacy.jdbc.model.ClusterInfo;
import com.bytedance.bitsail.connector.legacy.jdbc.model.ConnectionInfo;
import com.bytedance.bitsail.connector.legacy.jdbc.options.JdbcReaderOptions;
import com.bytedance.bitsail.connector.legacy.jdbc.options.JdbcWriterOptions;
import com.bytedance.bitsail.connector.legacy.jdbc.options.OracleReaderOptions;
import com.bytedance.bitsail.connector.legacy.jdbc.options.OracleWriterOptions;
import com.bytedance.bitsail.test.integration.AbstractIntegrationTest;
import com.bytedance.bitsail.test.integration.utils.JobConfUtils;

import com.google.common.collect.Lists;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;

import java.util.stream.Stream;

public class OracleConnectorITCase extends AbstractIntegrationTest {
  private static final Logger LOG = LoggerFactory.getLogger(OracleConnectorITCase.class);

  public static final String ORACLE_DOCKER_IMAGER = "gvenzl/oracle-xe:18.4.0-slim";

  private static final String SOURCE_TABLE = "ORACLE_TABLE_SOURCE";
  private static final String SINK_TABLE = "ORACLE_TABLE_SINK";

  private static OracleContainer oracleContainer;

  @BeforeClass
  public static void initDatabase() {
    oracleContainer = new OracleContainer(ORACLE_DOCKER_IMAGER)
        .withDatabaseName("TEST")
        .withUsername("TEST")
        .withPassword("TEST_PASSWORD")
        .withInitScript("oracle/init_database.sql")
        .withLogConsumer(new Slf4jLogConsumer(LOG));
    /*
     * This test may get Error of 'SP2-0306: Invalid option.' when running on Apple M chips.
     * Please follow instructions in 'OracleITCaseAppleChipWorkaround.md' for more details.
     */
    Startables.deepStart(Stream.of(oracleContainer)).join();
  }

  @AfterClass
  public static void closeDatabase() {
    oracleContainer.close();
  }

  @Test
  public void testOracleToPrint() throws Exception {
    BitSailConfiguration globalConfiguration = JobConfUtils.fromClasspath("oracle/oracle_source_to_print.json");
    globalConfiguration.set(OracleReaderOptions.TABLE_NAME, SOURCE_TABLE);

    ConnectionInfo connectionInfo = ConnectionInfo.builder()
        .host(oracleContainer.getHost())
        .port(oracleContainer.getFirstMappedPort())
        .url(oracleContainer.getJdbcUrl())
        .build();
    ClusterInfo clusterInfo = ClusterInfo.builder()
        .slave(connectionInfo)
        .build();
    globalConfiguration.set(JdbcReaderOptions.CONNECTIONS, Lists.newArrayList(clusterInfo));
    submitJob(globalConfiguration);
  }

  @Test
  public void testFakeToOracle() throws Exception {
    BitSailConfiguration globalConfiguration = JobConfUtils.fromClasspath("oracle/fake_to_oracle_sink.json");
    globalConfiguration.set(OracleWriterOptions.TABLE_NAME, SINK_TABLE);

    ConnectionInfo connectionInfo = new ConnectionInfo();
    connectionInfo.setHost(oracleContainer.getHost());
    connectionInfo.setPort(oracleContainer.getFirstMappedPort());
    connectionInfo.setUrl(oracleContainer.getJdbcUrl());
    globalConfiguration.set(JdbcWriterOptions.CONNECTIONS, Lists.newArrayList(connectionInfo));
    submitJob(globalConfiguration);
  }
}
