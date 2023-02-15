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
package com.bytedance.bitsail.connector.cdc.postgres.source.config;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.connector.cdc.jdbc.source.config.AbstractJdbcDebeziumConfig;
import com.bytedance.bitsail.connector.cdc.model.ConnectionInfo;
import com.bytedance.bitsail.connector.cdc.postgres.source.option.PostgresChangeEventOptions;
import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.relational.RelationalDatabaseConnectorConfig;

import java.util.Properties;

public class PostgresConfig extends AbstractJdbcDebeziumConfig {
  private final String pluginName;
  private final String slotName;
  private static final String PLUGIN_NAME_KEY = "plugin.name";
  private static final String SLOT_NAME_KEY = "slot.name";

  public PostgresConfig(BitSailConfiguration jobConf) {
    super(jobConf);
    this.pluginName = jobConf.get(PostgresChangeEventOptions.PLUGIN_NAME);
    this.slotName = jobConf.get(PostgresChangeEventOptions.SLOT_NAME);
  }

  @Override
  public RelationalDatabaseConnectorConfig getJdbcConnectorConfig(Configuration config) {
    return new PostgresConnectorConfig(config);
  }

  public void fillConnectionInfo(Properties props, ConnectionInfo connectionInfo, String timezone) {
    super.fillConnectionInfo(props, connectionInfo, timezone);

    props.put(PLUGIN_NAME_KEY, pluginName);
    props.put(SLOT_NAME_KEY, slotName);
  }
}
