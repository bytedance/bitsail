/*
 * Copyright 2022-2023 Bytedance Ltd. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.bytedance.bitsail.connector.cdc.mysql.source;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.connector.cdc.model.ClusterInfo;
import com.bytedance.bitsail.connector.cdc.model.ConnectionInfo;
import com.bytedance.bitsail.connector.cdc.mysql.source.container.MySQLContainerMariadbAdapter;
import com.bytedance.bitsail.connector.cdc.mysql.source.debezium.MysqlBinlogSplitReader;
import com.bytedance.bitsail.connector.cdc.mysql.source.schema.SchemaUtils;
import com.bytedance.bitsail.connector.cdc.option.BinlogReaderOptions;
import com.bytedance.bitsail.connector.cdc.source.offset.BinlogOffset;
import com.bytedance.bitsail.connector.cdc.source.split.BinlogSplit;

import com.google.common.collect.Lists;
import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnection;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.MysqlTextProtocolFieldReader;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;
import org.apache.kafka.connect.source.SourceRecord;
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

import java.time.ZoneId;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * This test will start a mysql container, and run some utils tests that need to connect mysql.
 */
public class MockConnectionsTest {
  private static final Logger LOG = LoggerFactory.getLogger(MockConnectionsTest.class);

  private static final String MYSQL_DOCKER_IMAGER = "mysql:8.0.29";

  private static final String TEST_USERNAME = "user1";
  private static final String TEST_PASSWORD = "password1";
  private static MySQLContainer<?> container;

  @BeforeClass
  public static void before() {
    container = new MySQLContainerMariadbAdapter<>(DockerImageName.parse(MYSQL_DOCKER_IMAGER))
        .withUrlParam("permitMysqlScheme", null)
        .withInitScript("scripts/insert_test.sql")
        .withUsername(TEST_USERNAME)
        .withPassword(TEST_PASSWORD)
        .withLogConsumer(new Slf4jLogConsumer(LOG));

    Startables.deepStart(Stream.of(container)).join();
  }

  @AfterClass
  public static void after() {
    container.close();
  }

  @Test
  public void testFetchSchemaToTableChanges() {
    Properties props = new Properties();
    props.put("database.hostname", container.getHost());
    props.put("database.port", String.valueOf(container.getFirstMappedPort()));
    props.put("database.user", TEST_USERNAME);
    props.put("database.password", TEST_PASSWORD);
    props.put("database.server.name", container.getHost());
    props.put("database.server.id", String.valueOf(container.getFirstMappedPort()));
    props.put("database.useSSL", "false");
    props.put("database.allowPublicKeyRetrieval", "true");
    props.put("database.serverTimezone", ZoneId.of("UTC").toString());
    props.put("database.history", "io.debezium.relational.history.MemoryDatabaseHistory");
    props.put("schema.history.internal", "io.debezium.relational.history.MemorySchemaHistory");
    props.put("include.schema.changes", "false");

    Configuration dbzConfiguration = Configuration.from(props);
    MySqlConnectorConfig connectorConfig = new MySqlConnectorConfig(dbzConfiguration);

    MySqlConnection connection = new MySqlConnection(
        new MySqlConnection.MySqlConnectionConfiguration(dbzConfiguration), new MysqlTextProtocolFieldReader());

    Map<TableId, TableChanges.TableChange> schema = SchemaUtils.discoverCapturedTableSchemas(connection, connectorConfig);
    Assert.assertEquals(1, schema.keySet().stream().filter(k -> k.toString().equals("test.jdbc_source_test")).count());
    Table table = schema.values().stream().findFirst().get().getTable();
    Assert.assertEquals(6, table.columns().size());
    Assert.assertTrue(table.primaryKeyColumnNames().contains("id"));
    Assert.assertEquals("utf8mb4", table.defaultCharsetName());
  }

  @Test
  public void testBinlogSplitReader() throws InterruptedException {
    BitSailConfiguration jobConf = BitSailConfiguration.newDefault();
    ConnectionInfo connectionInfo = ConnectionInfo.builder()
        .host(container.getHost())
        .port(container.getFirstMappedPort())
        .url(container.getJdbcUrl())
        .build();
    ClusterInfo clusterInfo = ClusterInfo.builder()
        .master(connectionInfo)
        .build();

    jobConf.set(BinlogReaderOptions.CONNECTIONS, Lists.newArrayList(clusterInfo));
    jobConf.set(BinlogReaderOptions.USER_NAME, "root");
    jobConf.set(BinlogReaderOptions.PASSWORD, TEST_PASSWORD);
    jobConf.set(BinlogReaderOptions.INITIAL_OFFSET_TYPE, "latest");

    MysqlBinlogSplitReader reader = new MysqlBinlogSplitReader(jobConf, 0, 1L);
    BinlogSplit split = new BinlogSplit("split-1", BinlogOffset.earliest(), BinlogOffset.boundless());
    reader.readSplit(split);
    int maxPeriod = 0;
    while (maxPeriod <= 5) {
      if (reader.hasNext()) {
        SourceRecord sourceRecord = reader.poll();
        LOG.info("source record: {}.", sourceRecord);
        maxPeriod++;
      }
      TimeUnit.SECONDS.sleep(1);
    }
    reader.close();
  }
}
