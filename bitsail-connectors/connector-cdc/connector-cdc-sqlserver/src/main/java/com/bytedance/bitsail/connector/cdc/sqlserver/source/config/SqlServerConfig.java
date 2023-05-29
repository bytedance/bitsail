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

package com.bytedance.bitsail.connector.cdc.sqlserver.source.config;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.connector.cdc.error.BinlogReaderErrorCode;
import com.bytedance.bitsail.connector.cdc.model.ClusterInfo;
import com.bytedance.bitsail.connector.cdc.model.ConnectionInfo;
import com.bytedance.bitsail.connector.cdc.option.BinlogReaderOptions;

import com.google.common.annotations.VisibleForTesting;
import io.debezium.config.Configuration;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig;
import lombok.Builder;
import lombok.Getter;
import org.apache.commons.lang.StringUtils;

import java.io.Serializable;
import java.time.ZoneId;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

@Getter
@Builder
public class SqlServerConfig implements Serializable {
  private static final long serialVersionUID = 1L;

  public static final String DEBEZIUM_PREFIX = "job.reader.debezium.";
  public static final String CHANGE_LSN = "change_lsn";
  public static final String COMMIT_LSN = "commit_lsn";
  public static final String EVENT_SERIAL_NO = "event_serial_no";

  private final String hostname;
  private final int port;
  private final String username;
  private final String password;
  private final String database;
  private final long instantId;

  // debezium configuration
  private final Properties dbzProperties;
  private final Configuration dbzConfiguration;
  private final SqlServerConnectorConfig dbzSqlServerConnectorConfig;

  @SuppressWarnings("checkstyle:MagicNumber")
  public static SqlServerConfig fromBitSailConf(BitSailConfiguration jobConf, long instantId) {
    List<ClusterInfo> clusterInfo = jobConf.getNecessaryOption(BinlogReaderOptions.CONNECTIONS, BinlogReaderErrorCode.REQUIRED_VALUE);
    //Only support one DB
    assert (clusterInfo.size() == 1);
    ConnectionInfo connectionInfo = clusterInfo.get(0).getMaster();
    assert (connectionInfo != null);
    Properties props = new Properties();
    fillDefaultProps(props);
    extractProps(props, jobConf);

    String username = jobConf.getNecessaryOption(BinlogReaderOptions.USER_NAME, BinlogReaderErrorCode.REQUIRED_VALUE);
    String password = jobConf.getNecessaryOption(BinlogReaderOptions.PASSWORD, BinlogReaderErrorCode.REQUIRED_VALUE);
    String database = jobConf.getNecessaryOption(BinlogReaderOptions.DB_NAME, BinlogReaderErrorCode.REQUIRED_VALUE);
    fillConnectionInfo(props, connectionInfo, username, password, database, instantId);

    Configuration config = Configuration.from(props);

    // be the same with debezium sqlserver connector
    // By default do not load whole result sets into memory
    config = config.edit()
        .withDefault("database.responseBuffering", "adaptive")
        .withDefault("database.fetchSize", 10_000)
        .build();

    return SqlServerConfig.builder()
        .hostname(connectionInfo.getHost())
        .port(connectionInfo.getPort())
        .username(username)
        .password(password)
        .database(database)
        .instantId(instantId)
        .dbzProperties(props)
        .dbzConfiguration(config)
        .dbzSqlServerConnectorConfig(new SqlServerConnectorConfig(config))
        .build();
  }

  @VisibleForTesting
  public static SqlServerConfig newDefault() {
    Properties props = new Properties();
    Configuration config = Configuration.from(props);
    return SqlServerConfig.builder()
        .hostname("")
        .port(0)
        .username("username")
        .password("password")
        .database("db")
        .instantId(1L)
        .dbzProperties(props)
        .dbzConfiguration(config)
        .dbzSqlServerConnectorConfig(new SqlServerConnectorConfig(config))
        .build();
  }

  /**
   * Extract debezium related properties from BitSail Configuration.
   */
  public static void extractProps(Properties props, BitSailConfiguration jobConf) {
    List<String> dbzConfKey = jobConf.getKeys().stream()
        .filter(s -> s.startsWith(DEBEZIUM_PREFIX))
        .map(s -> StringUtils.substringAfter(s, DEBEZIUM_PREFIX)).collect(Collectors.toList());
    for (String k : dbzConfKey) {
      String key = DEBEZIUM_PREFIX + k;
      String val = jobConf.getString(key);
      props.setProperty(k, val);
    }
  }

  /**
   * Set connection information to debezium properties.
   */
  public static void fillConnectionInfo(Properties props, ConnectionInfo connectionInfo, String username, String password, String database, long instantId) {
    props.put("database.hostname", connectionInfo.getHost());
    props.put("database.port", String.valueOf(connectionInfo.getPort()));
    props.put("database.user", username);
    props.put("database.password", password);
    props.put("database.dbname", database);
    props.put("database.server.name", String.valueOf(instantId));
    props.put("database.server.id", String.valueOf(instantId));
  }

  /**
   * Set up default debezium properties.
   * @param props
   */
  public static void fillDefaultProps(Properties props) {
    props.putIfAbsent("database.serverTimezone", ZoneId.of("UTC").toString());
    //TODO: standardize schema history for mysql and sqlserver
    props.putIfAbsent("database.history", "io.debezium.relational.history.MemoryDatabaseHistory");
    props.putIfAbsent("database.history.instance.name", "default_database_history");
    props.putIfAbsent("include.schema.changes", "false");
  }
}
