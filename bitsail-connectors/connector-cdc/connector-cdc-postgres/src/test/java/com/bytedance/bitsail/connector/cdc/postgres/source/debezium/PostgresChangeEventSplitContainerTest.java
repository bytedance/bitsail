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
package com.bytedance.bitsail.connector.cdc.postgres.source.debezium;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.connector.cdc.model.ClusterInfo;
import com.bytedance.bitsail.connector.cdc.model.ConnectionInfo;
import com.bytedance.bitsail.connector.cdc.option.BinlogReaderOptions;
import com.bytedance.bitsail.connector.cdc.postgres.container.PostgresContainerMariadbAdapter;
import com.bytedance.bitsail.connector.cdc.postgres.source.reader.PostgresChangeEventSplitReader;
import com.bytedance.bitsail.connector.cdc.postgres.source.util.TestDatabase;
import com.bytedance.bitsail.connector.cdc.source.offset.BinlogOffset;
import com.bytedance.bitsail.connector.cdc.source.split.BinlogSplit;
import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class PostgresChangeEventSplitContainerTest {
  private static final Logger LOG = LoggerFactory.getLogger(PostgresChangeEventSplitContainerTest.class);

  private static final String POSTGRESQL_DOCKER_IMAGER = "postgres:9.6.12";

  private static final String TEST_USERNAME = "user1";
  private static final String TEST_PASSWORD = "password1";
  private static final String TEST_DATABASE = "test";

  private PostgreSQLContainer<?> container;

  @Before
  public void before() {
    container = new PostgresContainerMariadbAdapter<>(DockerImageName.parse(POSTGRESQL_DOCKER_IMAGER))
            .withInitScript("scripts/jdbc_to_print.sql")
            .withUsername(TEST_USERNAME)
            .withPassword(TEST_PASSWORD)
            .withLogConsumer(new Slf4jLogConsumer(LOG));

    Startables.deepStart(Stream.of(container)).join();
  }

  @After
  public void after() {
    container.close();
  }

  @Test
  @Ignore
  public void testDatabaseConnection() {
    TestDatabase db = new TestDatabase(container, "test", TEST_USERNAME, TEST_PASSWORD);
    db.executeSql("SHOW TABLES;");
  }

  @Test
  @Ignore
  public void testBinlogReader() throws InterruptedException, IOException {
    BitSailConfiguration jobConf = BitSailConfiguration.newDefault();
    TestDatabase database = new TestDatabase(container, "test", TEST_USERNAME, TEST_PASSWORD);

    ConnectionInfo connectionInfo = ConnectionInfo.builder()
            .host(database.getPostgresSqlContainer().getHost())
            .port(database.getPostgresSqlContainer().getFirstMappedPort())
            .url(database.getPostgresSqlContainer().getJdbcUrl())
            .build();
    ClusterInfo clusterInfo = ClusterInfo.builder()
            .master(connectionInfo)
            .build();

    jobConf.set(BinlogReaderOptions.CONNECTIONS, Lists.newArrayList(clusterInfo));
    jobConf.set(BinlogReaderOptions.USER_NAME, database.getUsername());
    jobConf.set(BinlogReaderOptions.PASSWORD, database.getPassword());
    jobConf.set(BinlogReaderOptions.DB_NAME, database.getDatabaseName());
    jobConf.set("job.reader.debezium.database.allowPublicKeyRetrieval", "true");
    jobConf.set("job.reader.debezium.database.server.id", "123");
    jobConf.set("job.reader.debezium.database.server.name", "abc");
    jobConf.set("job.reader.debezium.gtid.source.filter.dml.events", "false");
    jobConf.set("job.reader.debezium.schema.history.internal", "io.debezium.relational.history.MemorySchemaHistory");
    jobConf.set("job.reader.debezium.database.history", "io.debezium.relational.history.MemoryDatabaseHistory");

    PostgresChangeEventSplitReader reader = new PostgresChangeEventSplitReader(jobConf, 0);
    BinlogSplit split = new BinlogSplit("split-1", BinlogOffset.earliest(), BinlogOffset.boundless());
    reader.readSplit(split);
    int maxPeriod = 0;
    while (maxPeriod <= 20) {
      if (reader.hasNext()) {
        reader.poll();
      }
      maxPeriod++;
      TimeUnit.SECONDS.sleep(1);
    }
    reader.close();
  }
}
