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

package com.bytedance.bitsail.test.e2e.datasource;

import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.exception.CommonErrorCode;
import com.bytedance.bitsail.common.option.ReaderOptions;
import com.bytedance.bitsail.common.util.Preconditions;
import com.bytedance.bitsail.connector.clickhouse.option.ClickhouseReaderOptions;
import com.bytedance.bitsail.connector.clickhouse.source.ClickhouseSource;
import com.bytedance.bitsail.test.e2e.executor.AbstractExecutor;

import com.clickhouse.jdbc.ClickHouseConnection;
import com.clickhouse.jdbc.ClickHouseDataSource;
import com.google.common.collect.Lists;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.ClickHouseContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.stream.Collectors;

public class ClickhouseDataSource extends AbstractDataSource {
  private static final Logger LOG = LoggerFactory.getLogger(ClickhouseDataSource.class);

  private static final int TOTAL_ROW_COUNT = 300;

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
          "    `p_date`        `Date`,\n" +
          "    `int_128`       `Int128`,\n" +
          "    `int_256`       `Int256`\n" +
          ") ENGINE = MergeTree()\n" +
          "PARTITION BY toYYYYMM(p_date)\n" +
          "PRIMARY KEY (id);";
  private static final String INSERT_SQL_HEADER = "INSERT INTO " + TABLE
      + " (id, int_type, double_type, string_type, p_date, int_128, int_256) VALUES ";
  private static final String COUNT_SQL = "SELECT count(id) FROM " + TABLE;
  private static final int INSERT_BATCH_SIZE = 10;

  private ClickHouseContainer container;

  private String hostAlias;

  @Override
  public String getContainerName() {
    return "data-source-clickhouse";
  }

  @Override
  public void initNetwork(Network executorNetwork) {
    this.network = executorNetwork;
  }

  @Override
  public void configure(BitSailConfiguration dataSourceConf) {
    // do nothing
  }

  @Override
  public boolean accept(BitSailConfiguration jobConf, Role role) {
    String readerClass = jobConf.get(ReaderOptions.READER_CLASS);
    if (role == Role.SOURCE) {
      return ClickhouseSource.class.getName().equals(readerClass);
    }
    return false;
  }

  @Override
  public void modifyJobConf(BitSailConfiguration jobConf) {
    jobConf.set(ClickhouseReaderOptions.JDBC_URL, getInternalJdbcHostUrl());
    jobConf.set(ClickhouseReaderOptions.DB_NAME, DATABASE);
    jobConf.set(ClickhouseReaderOptions.TABLE_NAME, TABLE);
    jobConf.set(ClickhouseReaderOptions.USER_NAME, container.getUsername());
    jobConf.set(ClickhouseReaderOptions.PASSWORD, container.getPassword());
  }

  @Override
  public void start() {
    hostAlias = getContainerName() + "-" + role;

    container = new ClickHouseContainer(DockerImageName.parse(CLICKHOUSE_IMAGE_NAME));
    container.withNetwork(network);
    container.withNetworkAliases(hostAlias);

    container.start();
    LOG.info("Clickhouse container starts! Host is: [{}], port is: [{}].", container.getHost(), HTTP_PORT);
  }

  @Override
  public void fillData(AbstractExecutor ignored) {
    List<String> values = new ArrayList<>();
    try {
      performQuery(DROP_TABLE_SQL);
      performQuery(CREATE_TABLE_SQL);

      for (int i = 0; i < TOTAL_ROW_COUNT; ++i) {
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
      Preconditions.checkState(TOTAL_ROW_COUNT == countTable(),
          "Something wrong when generating random data.");
    } catch (Exception e) {
      throw BitSailException.asBitSailException(CommonErrorCode.RUNTIME_ERROR,
          "Failed to generate random data.", e);
    }

    LOG.info("Successfully insert {} rows into table [{}]", TOTAL_ROW_COUNT, TABLE);
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
        new BigInteger("578960446186580977117854925043439539266349923328202820").toString()  // int256
    );
    return data.stream().collect(Collectors.joining(", ", "(", ")"));
  }

  public long countTable() throws SQLException {
    try (CloseableResultSet rs = performQuery(COUNT_SQL)) {
      ResultSet resultSet = rs.getResultSet();
      resultSet.next();
      return resultSet.getLong(1);
    }
  }

  private CloseableResultSet performQuery(String sql) throws SQLException {
    String jdbcUrl = container.getJdbcUrl();
    ClickHouseDataSource dataSource = new ClickHouseDataSource(jdbcUrl);
    ClickHouseConnection connection = dataSource.getConnection();
    Statement statement = connection.createStatement();
    statement.execute(sql);

    return new CloseableResultSet(statement.getResultSet(), connection);
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

  private String getInternalJdbcHostUrl() {
    return "jdbc:clickhouse://" + hostAlias + ":" + HTTP_PORT;
  }
}
