/*
 *     Licensed to the Apache Software Foundation (ASF) under one or more
 *     contributor license agreements.  See the NOTICE file distributed with
 *     this work for additional information regarding copyright ownership.
 *     The ASF licenses this file to You under the Apache License, Version 2.0
 *     (the "License"); you may not use this file except in compliance with
 *     the License.  You may obtain a copy of the License at
 *        http://www.apache.org/licenses/LICENSE-2.0
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 */

package com.bytedance.bitsail.connector.clickhouse;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.ClickHouseContainer;
import org.testcontainers.utility.DockerImageName;
import ru.yandex.clickhouse.BalancedClickhouseDataSource;
import ru.yandex.clickhouse.ClickHouseConnection;

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
  private static final String TABLE = "test_ch_table";
  private static final int HTTP_PORT = 8123;

  private static final String DROP_TABLE_SQL = "DROP TABLE IF EXISTS " + TABLE;
  private static final String CREATE_TABLE_SQL =
      "CREATE TABLE " + TABLE + "\n" +
      "(\n" +
      "    `id`            `Int64`,\n" +
      "    `int_type`      `Int32`,\n" +
      "    `double_type`   `Float64`,\n" +
      "    `string_type`   `String`,\n" +
      "    `p_date`        `Date`\n" +
      ") ENGINE = MergeTree()\n" +
      "PARTITION BY toYYYYMM(p_date)\n" +
      "PRIMARY KEY (id);";
  private static final String INSERT_SQL_HEADER = "INSERT INTO " + TABLE
      + " (id, int_type, double_type, string_type, p_date) VALUES ";
  private static final String COUNT_SQL = "SELECT count(id) FROM " + TABLE;
  private static final String SCAN_SQL = "SELECT id, int_type, double_type, string_type, p_date from " + TABLE + " ORDER BY id";
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
    return TABLE;
  }

  public String getUsername() {
    return container.getUsername();
  }

  public String getPassword() {
    return container.getPassword();
  }

  public void createExampleTable() throws SQLException {
    performQuery(DROP_TABLE_SQL);
    performQuery(CREATE_TABLE_SQL);
  }

  public long countTable() throws SQLException {
    ResultSet resultSet = performQuery(COUNT_SQL);
    resultSet.next();
    return resultSet.getLong(1);
  }

  public ResultSet scan() throws SQLException {
    return performQuery(SCAN_SQL);
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
    LOG.info("Successfully insert {} rows into table [{}]", totalCount, TABLE);
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
        "'" + sdf.format(calendar.getTime()) + "'"      // date_type
    );
    return data.stream().collect(Collectors.joining(", ", "(", ")"));
  }

  private ResultSet performQuery(String sql) throws SQLException {
    String jdbcUrl = container.getJdbcUrl();
    BalancedClickhouseDataSource dataSource = new BalancedClickhouseDataSource(jdbcUrl);
    try (ClickHouseConnection connection = dataSource.getConnection()) {
      Statement statement = connection.createStatement();
      statement.execute(sql);
      return statement.getResultSet();
    }
  }
}