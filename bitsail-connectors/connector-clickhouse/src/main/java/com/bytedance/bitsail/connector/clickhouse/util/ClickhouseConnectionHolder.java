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

package com.bytedance.bitsail.connector.clickhouse.util;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.connector.clickhouse.error.ClickhouseErrorCode;
import com.bytedance.bitsail.connector.clickhouse.option.ClickhouseReaderOptions;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.BalancedClickhouseDataSource;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.sql.SQLException;
import java.util.Properties;

public class ClickhouseConnectionHolder implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(ClickhouseConnectionHolder.class);

  private final String jdbcUrl;
  private final String dbName;

  private final String userName;
  private final String password;

  private final Properties connectionProperties;

  private ClickHouseConnection connection;

  public ClickhouseConnectionHolder(BitSailConfiguration jobConf) {
    this.jdbcUrl = jobConf.getNecessaryOption(ClickhouseReaderOptions.JDBC_URL,
        ClickhouseErrorCode.REQUIRED_VALUE);
    this.dbName = jobConf.getNecessaryOption(ClickhouseReaderOptions.CH_DATABASE_NAME,
        ClickhouseErrorCode.REQUIRED_VALUE);

    this.userName = jobConf.get(ClickhouseReaderOptions.USER_NAME);
    this.password = jobConf.get(ClickhouseReaderOptions.PASSWORD);

    this.connectionProperties = new Properties();
    if (jobConf.fieldExists(ClickhouseReaderOptions.CUSTOMIZED_CONNECTION_PROPERTIES)) {
      jobConf.get(ClickhouseReaderOptions.CUSTOMIZED_CONNECTION_PROPERTIES)
          .forEach(connectionProperties::setProperty);
    }
  }

  public ClickHouseConnection connect() {
    if (connection != null) {
      return connection;
    }

    String url = ClickhouseJdbcUtils.constructJdbcUrl(jdbcUrl, dbName);
    ClickHouseProperties properties = new ClickHouseProperties(connectionProperties);
    if (StringUtils.isNotEmpty(userName)) {
      properties.setUser(userName);
      properties.setPassword(password);
    }
    BalancedClickhouseDataSource dataSource = new BalancedClickhouseDataSource(url, properties);
    if (dataSource.getAllClickhouseUrls().size() > 1) {
      int enabledUrlSize = dataSource.actualize();
      LOG.info("There are {} urls enabled.", enabledUrlSize);
    }
    try {
      this.connection = dataSource.getConnection();
    } catch (SQLException e) {
      throw new RuntimeException("Failed to create connection.", e);
    }
    LOG.info("Connection created.");

    return connection;
  }

  @Override
  public void close() throws Exception {
    if (connection != null && !connection.isClosed()) {
      connection.close();
      LOG.info("Connection closed.");
    }
  }
}
