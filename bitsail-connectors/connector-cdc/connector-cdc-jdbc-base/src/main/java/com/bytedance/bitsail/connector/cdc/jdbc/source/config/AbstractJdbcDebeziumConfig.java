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
package com.bytedance.bitsail.connector.cdc.jdbc.source.config;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.connector.cdc.error.BinlogReaderErrorCode;
import com.bytedance.bitsail.connector.cdc.jdbc.source.constant.DebeziumConstant;
import com.bytedance.bitsail.connector.cdc.model.ClusterInfo;
import com.bytedance.bitsail.connector.cdc.model.ConnectionInfo;
import com.bytedance.bitsail.connector.cdc.option.BinlogReaderOptions;

import io.debezium.config.Configuration;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import lombok.Getter;
import org.apache.commons.lang.StringUtils;

import java.time.ZoneId;
import java.util.List;
import java.util.Properties;

@Getter
public abstract class AbstractJdbcDebeziumConfig {

  private static final long serialVersionUID = 1L;

  public static final String DEBEZIUM_PREFIX = "job.reader.debezium.";

  private final String hostname;
  private final int port;
  private final String username;
  private final String password;

  // debezium configuration
  private final Properties dbzProperties;
  private final Configuration dbzConfiguration;
  private final RelationalDatabaseConnectorConfig dbzJdbcConnectorConfig;
  private String dbName;

  public AbstractJdbcDebeziumConfig(BitSailConfiguration jobConf) {
    List<ClusterInfo> clusterInfo = jobConf.getNecessaryOption(BinlogReaderOptions.CONNECTIONS, BinlogReaderErrorCode.REQUIRED_VALUE);
    //Only support one DB
    assert (clusterInfo.size() == 1);
    ConnectionInfo connectionInfo = clusterInfo.get(0).getMaster();
    assert (connectionInfo != null);
    this.dbzProperties = extractProps(jobConf);
    this.hostname = connectionInfo.getHost();
    this.port = connectionInfo.getPort();
    this.username = jobConf.getNecessaryOption(BinlogReaderOptions.USER_NAME, BinlogReaderErrorCode.REQUIRED_VALUE);
    this.password = jobConf.getNecessaryOption(BinlogReaderOptions.PASSWORD, BinlogReaderErrorCode.REQUIRED_VALUE);
    this.dbName = jobConf.getNecessaryOption(BinlogReaderOptions.DB_NAME, BinlogReaderErrorCode.REQUIRED_VALUE);
    String timezone = jobConf.get(BinlogReaderOptions.CONNECTION_TIMEZONE);
    fillConnectionInfo(this.dbzProperties, connectionInfo, timezone);

    this.dbzConfiguration = Configuration.from(this.dbzProperties);
    this.dbzJdbcConnectorConfig = getJdbcConnectorConfig(this.dbzConfiguration);
  }

  public static Properties extractProps(BitSailConfiguration jobConf) {
    Properties props = new Properties();
    jobConf.getKeys().stream()
            .filter(s -> s.startsWith(DEBEZIUM_PREFIX))
            .map(s -> StringUtils.substringAfter(s, DEBEZIUM_PREFIX))
            .forEach(s -> props.setProperty(s, jobConf.getString(DEBEZIUM_PREFIX + s)));
    return props;
  }

  public abstract RelationalDatabaseConnectorConfig getJdbcConnectorConfig(Configuration config);

  public void fillConnectionInfo(Properties props, ConnectionInfo connectionInfo, String timezone) {
    props.put(DebeziumConstant.DATABASE_HOSTNAME, connectionInfo.getHost());
    props.put(DebeziumConstant.DATABASE_PORT, String.valueOf(connectionInfo.getPort()));
    props.put(DebeziumConstant.DATABASE_USER, username);
    props.put(DebeziumConstant.DATABASE_PASSWORD, password);
    props.put(DebeziumConstant.DATABASE_NAME, dbName);
    props.put(DebeziumConstant.DATABASE_TIMEZONE, ZoneId.of(timezone).toString());
  }

}
