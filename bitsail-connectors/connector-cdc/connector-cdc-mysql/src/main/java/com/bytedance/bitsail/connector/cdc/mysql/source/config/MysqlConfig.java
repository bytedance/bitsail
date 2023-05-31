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

package com.bytedance.bitsail.connector.cdc.mysql.source.config;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.connector.cdc.error.BinlogReaderErrorCode;
import com.bytedance.bitsail.connector.cdc.model.ClusterInfo;
import com.bytedance.bitsail.connector.cdc.model.ConnectionInfo;
import com.bytedance.bitsail.connector.cdc.option.BinlogReaderOptions;

import com.google.common.annotations.VisibleForTesting;
import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnectorConfig;
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
public class MysqlConfig implements Serializable {

  private static final long serialVersionUID = 1L;

  public static final String DEBEZIUM_PREFIX = "job.reader.debezium.";

  private final String hostname;
  private final int port;
  private final String username;
  private final String password;
  private final long instantId;

  // debezium configuration
  private final Properties dbzProperties;
  private final Configuration dbzConfiguration;
  private final MySqlConnectorConfig dbzMySqlConnectorConfig;

  public static MysqlConfig fromBitSailConf(BitSailConfiguration jobConf, long instantId) {
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
    fillConnectionInfo(props, connectionInfo, username, password, instantId);

    Configuration config = Configuration.from(props);
    return MysqlConfig.builder()
        .hostname(connectionInfo.getHost())
        .port(connectionInfo.getPort())
        .username(username)
        .password(password)
        .instantId(instantId)
        .dbzProperties(props)
        .dbzConfiguration(config)
        .dbzMySqlConnectorConfig(new MySqlConnectorConfig(config))
        .build();
  }

  @VisibleForTesting
  public static MysqlConfig newDefault() {
    Properties props = new Properties();
    Configuration config = Configuration.from(props);
    return MysqlConfig.builder()
        .hostname("")
        .port(0)
        .username("username")
        .password("password")
        .instantId(1L)
        .dbzProperties(props)
        .dbzConfiguration(config)
        .dbzMySqlConnectorConfig(new MySqlConnectorConfig(config))
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
  public static void fillConnectionInfo(Properties props, ConnectionInfo connectionInfo, String username, String password, long instantId) {
    props.put("database.hostname", connectionInfo.getHost());
    props.put("database.port", String.valueOf(connectionInfo.getPort()));
    props.put("database.user", username);
    props.put("database.password", password);
    props.put("database.server.name", String.valueOf(instantId));
    props.put("database.server.id", String.valueOf(instantId));
  }

  /**
   * Set up default debezium properties.
   * @param props
   */
  public static void fillDefaultProps(Properties props) {
    props.putIfAbsent("database.serverTimezone", ZoneId.of("UTC").toString());
    props.putIfAbsent("database.history", "com.bytedance.bitsail.connector.cdc.mysql.source.debezium.InMemoryDatabaseHistory");
    props.putIfAbsent("database.history.instance.name", "default_database_history");
    props.putIfAbsent("include.schema.changes", "false");
    props.putIfAbsent("database.useSSL", "false");
    props.putIfAbsent("database.allowPublicKeyRetrieval", "true");
  }
}
