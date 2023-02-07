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

package com.bytedance.bitsail.connector.legacy.jdbc.catalog;

import com.bytedance.bitsail.base.catalog.TableCatalogFactory;
import com.bytedance.bitsail.base.connector.BuilderGroup;
import com.bytedance.bitsail.base.execution.ExecutionEnviron;
import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.catalog.table.TableCatalog;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.exception.CommonErrorCode;
import com.bytedance.bitsail.common.exception.FrameworkErrorCode;
import com.bytedance.bitsail.connector.legacy.jdbc.exception.JDBCPluginErrorCode;
import com.bytedance.bitsail.connector.legacy.jdbc.model.ClusterInfo;
import com.bytedance.bitsail.connector.legacy.jdbc.model.ConnectionInfo;
import com.bytedance.bitsail.connector.legacy.jdbc.options.JdbcReaderOptions;
import com.bytedance.bitsail.connector.legacy.jdbc.options.JdbcWriterOptions;

import org.apache.commons.collections.CollectionUtils;

import java.util.List;

public class MySQLTableCatalogFactory implements TableCatalogFactory {

  @Override
  public TableCatalog createTableCatalog(BuilderGroup builderGroup,
                                         ExecutionEnviron executionEnviron,
                                         BitSailConfiguration connectorConfiguration) {
    if (BuilderGroup.READER.equals(builderGroup)) {

      List<ClusterInfo> connections = connectorConfiguration
          .getNecessaryOption(JdbcReaderOptions.CONNECTIONS,
              FrameworkErrorCode.REQUIRED_VALUE);

      return MySQLTableCatalog
          .builder()
          .database(connectorConfiguration.get(JdbcReaderOptions.DB_NAME))
          .table(connectorConfiguration.get(JdbcReaderOptions.TABLE_NAME))
          .schema(connectorConfiguration.get(JdbcReaderOptions.TABLE_SCHEMA))
          .username(connectorConfiguration.getNecessaryOption(JdbcReaderOptions.USER_NAME,
              CommonErrorCode.CONFIG_ERROR))
          .password(connectorConfiguration.getNecessaryOption(JdbcReaderOptions.PASSWORD,
              CommonErrorCode.CONFIG_ERROR))
          .customizedSQL(connectorConfiguration.get(JdbcReaderOptions.CUSTOMIZED_SQL))
          .url(getClusterUrl(connections))
          .build();
    }
    return MySQLTableCatalog
        .builder()
        .username(connectorConfiguration.get(JdbcWriterOptions.USER_NAME))
        .password(connectorConfiguration.get(JdbcWriterOptions.PASSWORD))
        .schema(connectorConfiguration.get(JdbcWriterOptions.USER_NAME))
        .table(connectorConfiguration.get(JdbcWriterOptions.TABLE_NAME))
        .database(connectorConfiguration.get(JdbcWriterOptions.DB_NAME))
        .url(connectorConfiguration.get(JdbcWriterOptions.CONNECTIONS)
            .get(0).getUrl())
        .build();
  }

  private static String getClusterUrl(List<ClusterInfo> connections) {
    if (CollectionUtils.isEmpty(connections)) {
      throw BitSailException.asBitSailException(
          JDBCPluginErrorCode.REQUIRED_VALUE,
          "Connection can't be empty.");
    }
    ClusterInfo clusterInfo = connections.get(0);
    ConnectionInfo connectionInfo = clusterInfo.getSlaves().get(0);
    return connectionInfo.getUrl();
  }

  @Override
  public String getComponentName() {
    return "JDBC";
  }
}
