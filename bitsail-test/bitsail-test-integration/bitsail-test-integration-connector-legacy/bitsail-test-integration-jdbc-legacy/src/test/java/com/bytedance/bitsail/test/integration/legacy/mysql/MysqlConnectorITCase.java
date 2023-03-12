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

package com.bytedance.bitsail.test.integration.legacy.mysql;

import com.bytedance.bitsail.common.catalog.table.CatalogTable;
import com.bytedance.bitsail.common.catalog.table.CatalogTableDefinition;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.connector.legacy.jdbc.catalog.MySQLTableCatalog;
import com.bytedance.bitsail.connector.legacy.jdbc.converter.JdbcTypeInfoConverter;
import com.bytedance.bitsail.connector.legacy.jdbc.model.ClusterInfo;
import com.bytedance.bitsail.connector.legacy.jdbc.model.ConnectionInfo;
import com.bytedance.bitsail.connector.legacy.jdbc.options.JdbcReaderOptions;
import com.bytedance.bitsail.connector.legacy.jdbc.options.JdbcWriterOptions;
import com.bytedance.bitsail.test.integration.AbstractIntegrationTest;
import com.bytedance.bitsail.test.integration.legacy.mysql.container.MySQLContainerMariadbAdapter;
import com.bytedance.bitsail.test.integration.utils.JobConfUtils;

import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import java.util.stream.Stream;

public class MysqlConnectorITCase extends AbstractIntegrationTest {

  private static final Logger LOG = LoggerFactory.getLogger(MysqlConnectorITCase.class);

  private static final String MYSQL_DOCKER_IMAGER = "mysql:8.0.29";

  private static final String DATABASE = "test";
  private static final String TABLE = "jdbc_dynamic_table";

  private static MySQLContainer<?> mySQLContainer;

  @BeforeClass
  public static void initMysqlDatabase() {
    mySQLContainer = new MySQLContainerMariadbAdapter<>(DockerImageName.parse(MYSQL_DOCKER_IMAGER))
        .withUrlParam("permitMysqlScheme", null)
        .withInitScript("mysql/init_database.sql")
        .withLogConsumer(new Slf4jLogConsumer(LOG));

    Startables.deepStart(Stream.of(mySQLContainer)).join();
  }

  @AfterClass
  public static void closeMysqlDatabase() {
    mySQLContainer.close();
  }

  @Test
  public void testInsertModeMysql() throws Exception {
    BitSailConfiguration globalConfiguration = JobConfUtils.fromClasspath("mysql/fake_to_mysql_sink.json");

    ConnectionInfo connectionInfo = new ConnectionInfo();
    connectionInfo.setHost(mySQLContainer.getHost());
    connectionInfo.setPort(mySQLContainer.getFirstMappedPort());
    connectionInfo.setUrl(mySQLContainer.getJdbcUrl());
    globalConfiguration.set(JdbcWriterOptions.CONNECTIONS, Lists.newArrayList(connectionInfo));
    submitJob(globalConfiguration);
  }

  @Test
  public void testMysqlReader() throws Exception {
    mysqlReader("mysql/mysql_to_print.json");
  }

  @Test
  public void testMysqlReaderWithoutColumns() throws Exception {
    mysqlReader("mysql/mysql_to_print_without_columns.json");
  }

  private void mysqlReader(String filePath) throws Exception {
    BitSailConfiguration jobConf = JobConfUtils.fromClasspath(filePath);

    ConnectionInfo connectionInfo = ConnectionInfo.builder()
        .host(mySQLContainer.getHost())
        .port(mySQLContainer.getFirstMappedPort())
        .url(mySQLContainer.getJdbcUrl())
        .build();
    ClusterInfo clusterInfo = ClusterInfo.builder()
        .slave(connectionInfo)
        .build();
    jobConf.set(JdbcReaderOptions.CONNECTIONS, Lists.newArrayList(clusterInfo));

    submitJob(jobConf);
  }

  @Test
  public void testGetCatalogTable() {
    MySQLTableCatalog catalog = MySQLTableCatalog
        .builder()
        .url(mySQLContainer.getJdbcUrl())
        .table(TABLE)
        .database(DATABASE)
        .username(mySQLContainer.getUsername())
        .password(mySQLContainer.getPassword())
        .build();

    catalog.open(new JdbcTypeInfoConverter("mysql"));

    CatalogTableDefinition catalogTableDefinition = catalog.createCatalogTableDefinition();
    CatalogTable catalogTable = catalog.getCatalogTable(catalogTableDefinition);

    Assert.assertNotNull(catalogTable.getCatalogTableSchema());
    Assert.assertEquals(CollectionUtils.size(catalogTable.getCatalogTableSchema().getColumns()), 21);
  }
}
