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

package com.bytedance.bitsail.connector.cdc.mysql.source.debezium;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.connector.cdc.model.ClusterInfo;
import com.bytedance.bitsail.connector.cdc.model.ConnectionInfo;
import com.bytedance.bitsail.connector.cdc.option.BinlogReaderOptions;
import com.bytedance.bitsail.connector.cdc.source.offset.BinlogOffset;
import com.bytedance.bitsail.connector.cdc.source.split.BinlogSplit;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

public class MysqlBinlogSplitReaderTest {
  private static final Logger LOG = LoggerFactory.getLogger(MysqlBinlogSplitReaderTest.class);
  String username = "root";
  String password = "pw";
  String host = "localhost";
  int port = 54000;

  //@Test
  public void testConnection() throws SQLException {
    Connection connection = DriverManager.getConnection(
        getJdbcUrl(), username, password);
    Statement statement = connection.createStatement();
    statement.execute("SHOW DATABASES;");
  }

  //@Test
  public void testReader() throws InterruptedException {
    BitSailConfiguration jobConf = BitSailConfiguration.newDefault();

    ConnectionInfo connectionInfo = ConnectionInfo.builder()
        .host(host)
        .port(port)
        .url(getJdbcUrl())
        .build();
    ClusterInfo clusterInfo = ClusterInfo.builder()
        .master(connectionInfo)
        .build();

    jobConf.set(BinlogReaderOptions.CONNECTIONS, Lists.newArrayList(clusterInfo));
    jobConf.set(BinlogReaderOptions.USER_NAME, username);
    jobConf.set(BinlogReaderOptions.PASSWORD, password);
    jobConf.set("job.reader.debezium.database.useSSL", "false");
    jobConf.set("job.reader.debezium.database.allowPublicKeyRetrieval", "true");
    jobConf.set("job.reader.debezium.database.server.id", "123");
    jobConf.set("job.reader.debezium.database.server.name", "abc");
    jobConf.set("job.reader.debezium.schema.history.internal", "io.debezium.relational.history.MemorySchemaHistory");
    jobConf.set("job.reader.debezium.database.history", "io.debezium.relational.history.MemoryDatabaseHistory");
    jobConf.set("job.reader.debezium.include.schema.changes", "false");

    MysqlBinlogSplitReader reader = new MysqlBinlogSplitReader(jobConf, 0);
    BinlogSplit split = new BinlogSplit("split-1", BinlogOffset.earliest(), BinlogOffset.boundless());
    reader.readSplit(split);
    int maxPeriod = 0;
    while (maxPeriod <= 25) {
      if (reader.hasNext()) {
        Row row = reader.poll();
        Arrays.stream(row.getFields()).forEach(o -> LOG.info(o.toString()));
        maxPeriod++;
      }
      TimeUnit.SECONDS.sleep(1);
    }

  }

  public String getJdbcUrl() {
    return "jdbc:mysql://" + host + ":" + port;
  }
}
