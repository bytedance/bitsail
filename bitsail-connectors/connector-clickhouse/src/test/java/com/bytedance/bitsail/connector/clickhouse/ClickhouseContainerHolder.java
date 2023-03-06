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

package com.bytedance.bitsail.connector.clickhouse;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseCredentials;
import com.clickhouse.client.ClickHouseFile;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.jdbc.ClickHouseConnection;
import com.clickhouse.jdbc.ClickHouseDataSource;
import com.google.common.collect.Lists;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.ClickHouseContainer;
import org.testcontainers.utility.DockerImageName;

import java.math.BigInteger;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.stream.Collectors;

public class ClickhouseContainerHolder {
  private static final Logger LOG = LoggerFactory.getLogger(ClickhouseContainerHolder.class);

  private static final String CLICKHOUSE_IMAGE_NAME = "clickhouse/clickhouse-server:22-alpine";
  private static final String DATABASE = "default";
  private static final String TABLE_SOURCE = "test_ch_table";
  private static final String TABLE_SINK = "test_ch_table_sink";
  private static final int HTTP_PORT = 8123;

  private static final String DROP_SOURCE_TABLE_SQL = "DROP TABLE IF EXISTS " + TABLE_SOURCE;
  private static final String DROP_SINK_TABLE_SQL = "DROP TABLE IF EXISTS " + TABLE_SINK;
  private static final String CREATE_SOURCE_TABLE_SQL =
          "CREATE TABLE " + TABLE_SOURCE + "\n" +
                  "(\n" +
                  "    `id`            `Int64`,\n" +
                  "    `int_type`      `Int32`,\n" +
                  "    `double_type`   `Float64`,\n" +
                  "    `string_type`   `String`,\n" +
                  "    `p_date`        `Date`,\n" +
                  "    `int_128`       `Int128`,\n" +
                  "    `int_256`       `Int256`,\n" +
                  "    `decimal_type`  Decimal256(76)\n" +
                  ") ENGINE = MergeTree()\n" +
                  "PARTITION BY toYYYYMM(p_date)\n" +
                  "PRIMARY KEY (id);";
  private static final String CREATE_SINK_TABLE_SQL =
      "CREATE TABLE " + TABLE_SINK + "\n" +
      "(\n" +
      "    `id`            Nullable(Int64),\n" +
      "    `int_type`      Nullable(Int32),\n" +
      "    `double_type`   Nullable(Float64),\n" +
      "    `string_type`   Nullable(String),\n" +
      "    `p_date`        Nullable(Date),\n" +
      "    `int_128`       Nullable(Int128),\n" +
      "    `int_256`       Nullable(Int256),\n" +
      "    `decimal_type`  Nullable(Decimal256(76))\n" +
      ") ENGINE = TinyLog\n";
  private static final String INSERT_SQL_HEADER = "INSERT INTO " + TABLE_SOURCE
          + " (id, int_type, double_type, string_type, p_date, int_128, int_256,decimal_type) VALUES ";
  private static final String COUNT_SQL = "SELECT count(id) FROM " + TABLE_SOURCE;
  private static final String SCAN_SQL = "SELECT id, int_type, double_type, string_type, p_date, int_128, int_256,decimal_type from " + TABLE_SOURCE + " ORDER BY id";
  private static final int INSERT_BATCH_SIZE = 10;

  private ClickHouseContainer container;

  public void initContainer() {
    if (container == null) {
      container = new ClickHouseContainer(DockerImageName.parse(CLICKHOUSE_IMAGE_NAME));
    }
  }

  public void start() {
    initContainer();
    container.start();
  }

  public void close() {
    container.close();
    container = null;
  }

  public String getJdbcHostUrl() {
    return "jdbc:clickhouse://" + container.getHost() + ":" + container.getMappedPort(HTTP_PORT);
  }

  public String getDatabase() {
    return DATABASE;
  }

  public String getTable() {
    return TABLE_SOURCE;
  }

  public String getUsername() {
    return container.getUsername();
  }

  public String getPassword() {
    return container.getPassword();
  }

  public void createExampleSourceTable() throws SQLException {
    performQuery(DROP_SOURCE_TABLE_SQL);
    performQuery(CREATE_SOURCE_TABLE_SQL);
  }

  public void createExampleSinkTable() throws SQLException {
    performQuery(DROP_SINK_TABLE_SQL);
    performQuery(CREATE_SINK_TABLE_SQL);
  }

  public long countTable() throws SQLException {
    try (CloseableResultSet rs = performQuery(COUNT_SQL)) {
      ResultSet resultSet = rs.getResultSet();
      resultSet.next();
      return resultSet.getLong(1);
    }
  }

  public void insertData(int totalCount) throws Exception {
    List<String> values = new ArrayList<>();
    for (int i = 0; i < totalCount; ++i) {
      values.add(generateRow(i));
      if ((i + 1) % INSERT_BATCH_SIZE == 0) {
        String insertSql = values.stream().collect(Collectors.joining(", ", INSERT_SQL_HEADER, ";"));
        performQuery(insertSql);
        values = new ArrayList<>();
      }
    }

    if (!values.isEmpty()) {
      String insertSql = values.stream().collect(Collectors.joining(", ", INSERT_SQL_HEADER, ";"));
      performQuery(insertSql);
    }
    LOG.info("Successfully insert {} rows into table [{}]", totalCount, TABLE_SOURCE);
  }

  public void importData() throws Exception {
    ClickHouseNode server = getServer();
    Path csvFile = Paths.get(this.getClass().getClassLoader().getResource("example_data.csv").getPath());
    ClickHouseFile file = ClickHouseFile.of(csvFile);

    try (ClickHouseClient client = ClickHouseClient.newInstance(server.getProtocol())) {
      client.connect(server).write()
          .table(TABLE_SOURCE)
          .data(file)
          .executeAndWait();
    }

    long count = countTable();
    LOG.info("Successfully import {} rows", count);
  }

  @SuppressWarnings("checkstyle:MagicNumber")
  private String generateRow(int index) throws Exception {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    Calendar calendar = Calendar.getInstance();
    calendar.setTime(sdf.parse("2022-01-01"));
    calendar.add(Calendar.DAY_OF_YEAR, index);

    List<String> data = Lists.newArrayList(
        "" + index,            // id
        Integer.valueOf(100000 + index).toString(),             // int_type
        Double.valueOf((100000.0 + index) / 1000).toString(),  // double_type
        "'text_" + index + "'",            // string_type
        "'" + sdf.format(calendar.getTime()) + "'",      // date_type
        new BigInteger("17014118346046923173168730371588").toString(),  // int128
        new BigInteger("578960446186580977117854925043439539266349923328202820").toString(),  // int256
        Double.valueOf((100000.0 + index) / 1000000.0).toString()   //decimal_type
    );
    return data.stream().collect(Collectors.joining(", ", "(", ")"));
  }

  private CloseableResultSet performQuery(String sql) throws SQLException {
    String jdbcUrl = container.getJdbcUrl();
    ClickHouseDataSource dataSource = new ClickHouseDataSource(jdbcUrl);
    ClickHouseConnection connection = dataSource.getConnection();
    Statement statement = connection.createStatement();
    statement.execute(sql);

    return new CloseableResultSet(statement.getResultSet(), connection);
  }

  private ClickHouseNode getServer() {
    return ClickHouseNode.builder()
        .host("localhost")
        .port(ClickHouseProtocol.HTTP, container.getMappedPort(HTTP_PORT))
        .database("default")
        .credentials(ClickHouseCredentials.fromUserAndPassword(container.getUsername(), container.getPassword()))
        .build();
  }

  @AllArgsConstructor
  private static class CloseableResultSet implements AutoCloseable {
    @Getter
    private final ResultSet resultSet;
    private final Connection connection;

    @Override
    public void close() throws SQLException {
      if (connection != null) {
        connection.close();
      }
    }
  }
}
