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

import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import lombok.Builder;
import lombok.Getter;
import org.apache.commons.lang.StringUtils;

import java.io.Serializable;
import java.time.ZoneId;
import java.util.List;
import java.util.Properties;

@Getter
@Builder
public class MysqlConfig implements Serializable {

  private static final long serialVersionUID = 1L;

  public static final String DEBEZIUM_PREFIX = "job.reader.debezium.";

  private final String hostname;
  private final int port;
  private final String username;
  private final String password;

  // debezium configuration
  private final Properties dbzProperties;
  private final Configuration dbzConfiguration;
  private final MySqlConnectorConfig dbzMySqlConnectorConfig;

  public static MysqlConfig fromBitSailConf(BitSailConfiguration jobConf) {
    List<ClusterInfo> clusterInfo = jobConf.getNecessaryOption(BinlogReaderOptions.CONNECTIONS, BinlogReaderErrorCode.REQUIRED_VALUE);
    //Only support one DB
    assert (clusterInfo.size() == 1);
    ConnectionInfo connectionInfo = clusterInfo.get(0).getMaster();
    assert (connectionInfo != null);
    Properties props = extractProps(jobConf);
    String username = jobConf.getNecessaryOption(BinlogReaderOptions.USER_NAME, BinlogReaderErrorCode.REQUIRED_VALUE);
    String password = jobConf.getNecessaryOption(BinlogReaderOptions.PASSWORD, BinlogReaderErrorCode.REQUIRED_VALUE);
    fillConnectionInfo(props, connectionInfo, username, password);

    Configuration config = Configuration.from(props);
    return MysqlConfig.builder()
        .hostname(connectionInfo.getHost())
        .port(connectionInfo.getPort())
        .username(username)
        .password(password)
        .dbzProperties(props)
        .dbzConfiguration(config)
        .dbzMySqlConnectorConfig(new MySqlConnectorConfig(config))
        .build();
  }

  public static Properties extractProps(BitSailConfiguration jobConf) {
    Properties props = new Properties();
    jobConf.getKeys().stream()
        .filter(s -> s.startsWith(DEBEZIUM_PREFIX))
            .map(s -> StringUtils.substringAfter(s, DEBEZIUM_PREFIX))
                .forEach(s -> props.setProperty(s, jobConf.getString(DEBEZIUM_PREFIX + s)));
    return props;
  }

  public static void fillConnectionInfo(Properties props, ConnectionInfo connectionInfo, String username, String password) {
    props.put("database.hostname", connectionInfo.getHost());
    props.put("database.port", String.valueOf(connectionInfo.getPort()));
    props.put("database.user", username);
    props.put("database.password", password);
    props.put("database.serverTimezone", ZoneId.of("UTC").toString());
  }

}
