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

package com.bytedance.bitsail.connector.cdc.sqlserver.source.reader;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.connector.cdc.model.ClusterInfo;
import com.bytedance.bitsail.connector.cdc.model.ConnectionInfo;
import com.bytedance.bitsail.connector.cdc.option.BinlogReaderOptions;
import com.bytedance.bitsail.connector.cdc.source.offset.BinlogOffset;
import com.bytedance.bitsail.connector.cdc.source.split.BinlogSplit;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class SqlServerBinlogSplitReaderTest {
  private static final Logger LOG = LoggerFactory.getLogger(SqlServerBinlogSplitReaderTest.class);

  private static MSSQLServerContainer<?> container;

  private static final String USERNAME = "SA";
  private static final String PASSWORD = "Password!";
  private static final String DATABASE = "bitsail_test";

  private static ExecutorService executorService;

  @BeforeClass
  public static void before() throws SQLException {
    container = new MSSQLServerContainer<>("mcr.microsoft.com/mssql/server:2019-latest")
        .withPassword(PASSWORD)
        .withInitScript("scripts/test_insert_full_type.sql")
        .withEnv("MSSQL_AGENT_ENABLED", "true")
        .withEnv("ACCEPT_EULA", "Y")
        .withEnv("MSSQL_PID", "Standard")
        .withLogConsumer(new Slf4jLogConsumer(LOG));

    Startables.deepStart(Stream.of(container)).join();

    ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("test-data-generator").build();
    executorService = Executors.newSingleThreadExecutor(threadFactory);
    Connection connection = DriverManager.getConnection(
        container.getJdbcUrl(), container.getUsername(), container.getPassword());
    Statement statement = connection.createStatement();
    statement.execute("USE bitsail_test;");
    executorService.submit(() -> {
      try {
        int testBatch = 1000;
        int i = 0;
        while (i < testBatch) {
          statement.execute("INSERT INTO ExampleTable (BitColumn, TinyIntColumn, SmallIntColumn, IntColumn, BigIntColumn, DecimalColumn, NumericColumn," +
              " SmallMoneyColumn, MoneyColumn, FloatColumn, RealColumn, DateColumn, TimeColumn, DateTimeColumn, DateTime2Column, DateTimeOffsetColumn, CharColumn," +
              " VarcharColumn, TextColumn, NCharColumn, NVarcharColumn, NTextColumn, BinaryColumn, VarbinaryColumn, ImageColumn, XMLColumn)\n" +
              "VALUES (1, 255, 32767, 2147483647, 9223372036854775807, 1234.56, 1234.56, 12.34, 1234.56, 1234.56, 1234.56," +
              " '2023-05-19', '12:34:56.789', '2023-05-19 12:34:56'," +
              " '2023-05-19 12:34:56.789', '2023-05-19 12:34:56.789-07:00', 'ABCDEF', 'This is a varchar'," +
              " 'This is a text', N'char', N'This is a nvarchar', N'This is a ntext'," +
              " 0x1234567890, 0x1234567890, 0x1234567890, '<a>b</a>');\n");
          TimeUnit.SECONDS.sleep(1);
          i++;
        }

      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
  }

  @AfterClass
  public static void after() {
    container.close();
    executorService.shutdown();
  }

  @Test
  public void testBinlogReader() throws InterruptedException {
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
    jobConf.set(BinlogReaderOptions.USER_NAME, USERNAME);
    jobConf.set(BinlogReaderOptions.PASSWORD, PASSWORD);
    jobConf.set(BinlogReaderOptions.INITIAL_OFFSET_TYPE, "earliest");
    jobConf.set(BinlogReaderOptions.DB_NAME, DATABASE);
    //jobConf.set("job.reader.debezium.table.whitelist", "dbo.ExampleTable");

    SqlServerBinlogSplitReader reader = new SqlServerBinlogSplitReader(jobConf, 0, 1L);
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
