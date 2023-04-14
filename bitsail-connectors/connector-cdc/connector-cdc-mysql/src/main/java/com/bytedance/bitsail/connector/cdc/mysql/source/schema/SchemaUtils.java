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

package com.bytedance.bitsail.connector.cdc.mysql.source.schema;

import com.bytedance.bitsail.connector.cdc.mysql.source.sql.MysqlFieldDescriber;

import io.debezium.connector.mysql.MySqlConnection;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.RelationalTableFilters;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utils to get schema.
 */
public class SchemaUtils {
  private static final Logger LOG = LoggerFactory.getLogger(SchemaUtils.class);
  public static List<TableId> listTables(JdbcConnection jdbc, RelationalTableFilters tableFilters)
      throws SQLException {
    final List<TableId> capturedTableIds = new ArrayList<>();
    // -------------------
    // READ DATABASE NAMES
    // -------------------
    // Get the list of databases ...
    LOG.info("Read list of available databases");
    final List<String> databaseNames = new ArrayList<>();

    jdbc.query(
        "SHOW DATABASES",
        rs -> {
          while (rs.next()) {
            databaseNames.add(rs.getString(1));
          }
        });
    LOG.info("\t list of available databases is: {}", databaseNames);

    // ----------------
    // READ TABLE NAMES
    // ----------------
    // Get the list of table IDs for each database. We can't use a prepared statement with
    // MySQL, so we have to build the SQL statement each time. Although in other cases this
    // might lead to SQL injection, in our case we are reading the database names from the
    // database and not taking them from the user ...
    LOG.info("Read list of available tables in each database");
    for (String dbName : databaseNames) {
      try {
        jdbc.query(
            "SHOW FULL TABLES IN " + MysqlFieldDescriber.quote(dbName) + " where Table_Type = 'BASE TABLE'",
            rs -> {
              while (rs.next()) {
                TableId tableId = new TableId(dbName, null, rs.getString(1));
                if (tableFilters.dataCollectionFilter().isIncluded(tableId)) {
                  capturedTableIds.add(tableId);
                  LOG.info("\t including '{}' for further processing", tableId);
                } else {
                  LOG.info("\t '{}' is filtered out of capturing", tableId);
                }
              }
            });
      } catch (SQLException e) {
        // We were unable to execute the query or process the results, so skip this ...
        LOG.warn(
            "\t skipping database '{}' due to error reading tables: {}",
            dbName,
            e.getMessage());
      }
    }
    return capturedTableIds;
  }

  public static Map<TableId, TableChanges.TableChange> discoverCapturedTableSchemas(
      MySqlConnection jdbc, MySqlConnectorConfig connectorConfig) {
    RelationalTableFilters tableFilters = connectorConfig.getTableFilters();
    final List<TableId> capturedTableIds;
    try {
      capturedTableIds = listTables(jdbc, tableFilters);
    } catch (SQLException e) {
      throw new RuntimeException("Failed to discover captured tables", e);
    }
    if (capturedTableIds.isEmpty()) {
      throw new IllegalArgumentException(
          String.format(
              "Can't find any matched tables, please check your configured database-name: %s and table-name: %s",
              "database", "table"));
    }

    // fetch table schemas
    MySqlSchemaFetcher mySqlSchemaFetcher = new MySqlSchemaFetcher(connectorConfig, jdbc.isTableIdCaseSensitive());
    Map<TableId, TableChanges.TableChange> tableSchemas = new HashMap<>();
    for (TableId tableId : capturedTableIds) {
      TableChanges.TableChange tableSchema = mySqlSchemaFetcher.getTableSchema(jdbc, tableId);
      tableSchemas.put(tableId, tableSchema);
    }
    return tableSchemas;
  }

  public static String quoteTableId(TableId tableId) {
    return tableId.toQuotedString('`');
  }
}
