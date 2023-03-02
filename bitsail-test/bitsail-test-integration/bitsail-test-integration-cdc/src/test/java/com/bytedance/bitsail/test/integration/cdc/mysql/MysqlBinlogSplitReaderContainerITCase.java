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

package com.bytedance.bitsail.test.integration.cdc.mysql;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.connector.cdc.model.ClusterInfo;
import com.bytedance.bitsail.connector.cdc.model.ConnectionInfo;
import com.bytedance.bitsail.connector.cdc.mysql.source.debezium.MysqlBinlogSplitReader;
import com.bytedance.bitsail.connector.cdc.option.BinlogReaderOptions;
import com.bytedance.bitsail.connector.cdc.source.offset.BinlogOffset;
import com.bytedance.bitsail.connector.cdc.source.split.BinlogSplit;
import com.bytedance.bitsail.test.integration.cdc.mysql.container.MySQLContainerMariadbAdapter;
import com.bytedance.bitsail.test.integration.cdc.mysql.container.util.TestDatabase;

import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class MysqlBinlogSplitReaderContainerITCase {

  private static final Logger LOG = LoggerFactory.getLogger(MysqlBinlogSplitReaderContainerITCase.class);

  private static final String MYSQL_DOCKER_IMAGER = "mysql:8.0.29";

  private static final String TEST_USERNAME = "user1";
  private static final String TEST_PASSWORD = "password1";
  private static final String TEST_DATABASE = "test";

  private MySQLContainer<?> container;

  @Before
  public void before() {
    container = new MySQLContainerMariadbAdapter<>(DockerImageName.parse(MYSQL_DOCKER_IMAGER))
        //.withConfigurationOverride("container/my.cnf")
        //.withUrlParam("permitMysqlScheme", null)
        .withInitScript("scripts/jdbc_to_print.sql")
        //.withDatabaseName(TEST_DATABASE)
        .withUsername(TEST_USERNAME)
        .withPassword(TEST_PASSWORD)
        .withLogConsumer(new Slf4jLogConsumer(LOG));
    //container.addParameter("MY_CNF", "container/my.cnf");

    Startables.deepStart(Stream.of(container)).join();
  }

  @After
  public void after() {
    container.close();
  }

  //@Test
  public void testDatabaseConnection() {
    TestDatabase db = new TestDatabase(container, "test", TEST_USERNAME, TEST_PASSWORD);
    db.executeSql("SHOW TABLES;");
  }

  //@Test
  public void testBinlogReader() throws InterruptedException {
    BitSailConfiguration jobConf = BitSailConfiguration.newDefault();
    TestDatabase database = new TestDatabase(container, "test", TEST_USERNAME, TEST_PASSWORD);

    ConnectionInfo connectionInfo = ConnectionInfo.builder()
        .host(database.getMySQLContainer().getHost())
        .port(database.getMySQLContainer().getFirstMappedPort())
        .url(database.getMySQLContainer().getJdbcUrl())
        .build();
    ClusterInfo clusterInfo = ClusterInfo.builder()
        .master(connectionInfo)
        .build();

    jobConf.set(BinlogReaderOptions.CONNECTIONS, Lists.newArrayList(clusterInfo));
    jobConf.set(BinlogReaderOptions.USER_NAME, database.getUsername());
    jobConf.set(BinlogReaderOptions.PASSWORD, database.getPassword());
    jobConf.set("job.reader.debezium.database.allowPublicKeyRetrieval", "true");
    jobConf.set("job.reader.debezium.database.server.id", "123");
    jobConf.set("job.reader.debezium.database.server.name", "abc");
    jobConf.set("job.reader.debezium.gtid.source.filter.dml.events", "false");
    jobConf.set("job.reader.debezium.schema.history.internal", "io.debezium.relational.history.MemorySchemaHistory");
    jobConf.set("job.reader.debezium.database.history", "io.debezium.relational.history.MemoryDatabaseHistory");

    MysqlBinlogSplitReader reader = new MysqlBinlogSplitReader(jobConf, 0);
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
