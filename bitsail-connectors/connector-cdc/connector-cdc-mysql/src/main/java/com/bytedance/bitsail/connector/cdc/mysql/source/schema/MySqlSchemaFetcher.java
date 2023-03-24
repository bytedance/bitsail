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

import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.connector.cdc.error.BinlogReaderErrorCode;
import com.bytedance.bitsail.connector.cdc.mysql.source.sql.MysqlFieldDescriber;
import com.bytedance.bitsail.connector.cdc.mysql.source.sql.MysqlTableDescriber;

import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.MySqlDatabaseSchema;
import io.debezium.connector.mysql.MySqlOffsetContext;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;
import io.debezium.schema.SchemaChangeEvent;
import org.apache.commons.lang3.StringUtils;

import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.bytedance.bitsail.connector.cdc.mysql.source.debezium.DebeziumHelper.createMySqlDatabaseSchema;
import static com.bytedance.bitsail.connector.cdc.mysql.source.schema.SchemaUtils.quoteTableId;

/**
 * Fetch mysql schema by executing SHOW CREATE TABLE statement.
 */
public class MySqlSchemaFetcher {
  private static final String SHOW_CREATE_TABLE = "SHOW CREATE TABLE ";
  private static final String DESC_TABLE = "DESC ";

  private final MySqlConnectorConfig connectorConfig;
  private final MySqlDatabaseSchema databaseSchema;
  private final Map<TableId, TableChanges.TableChange> schemasByTableId;

  public MySqlSchemaFetcher(MySqlConnectorConfig connectorConfig, boolean isTableIdCaseSensitive) {
    this.connectorConfig = connectorConfig;
    this.databaseSchema = createMySqlDatabaseSchema(connectorConfig, isTableIdCaseSensitive);
    this.schemasByTableId = new HashMap<>();
  }

  /**
   * Gets table schema for the given table path. It will request to MySQL server by running `SHOW
   * CREATE TABLE` if cache missed.
   */
  public TableChanges.TableChange getTableSchema(JdbcConnection jdbc, TableId tableId) {
    // read schema from cache first
    TableChanges.TableChange schema = schemasByTableId.get(tableId);
    if (schema == null) {
      schema = buildTableSchema(jdbc, tableId);
      schemasByTableId.put(tableId, schema);
    }
    return schema;
  }

  private TableChanges.TableChange buildTableSchema(JdbcConnection jdbc, TableId tableId) {
    final Map<TableId, TableChanges.TableChange> tableChangeMap = new HashMap<>();
    //Step 1: try to get schema from SHOW CREATE TABLE
    String showCreateTable = SHOW_CREATE_TABLE + quoteTableId(tableId);
    buildSchemaByShowCreateTable(jdbc, tableId, tableChangeMap);
    //Step 2: fallback to DESC table
    if (!tableChangeMap.containsKey(tableId)) {
      String descTable = DESC_TABLE + quoteTableId(tableId);
      buildSchemaByDescTable(jdbc, descTable, tableId, tableChangeMap);
      if (!tableChangeMap.containsKey(tableId)) {
        throw new BitSailException(BinlogReaderErrorCode.SQL_ERROR, String.format(
            "Can't obtain schema for table %s by running %s and %s ",
            tableId, showCreateTable, descTable));
      }
    }
    return tableChangeMap.get(tableId);
  }

  private void buildSchemaByShowCreateTable(
      JdbcConnection jdbc, TableId tableId, Map<TableId, TableChanges.TableChange> tableChangeMap) {
    final String sql = SHOW_CREATE_TABLE + quoteTableId(tableId);
    try {
      jdbc.query(
          sql,
          rs -> {
            if (rs.next()) {
              final String ddl = rs.getString(2);
              parseSchemaByDdl(ddl, tableId, tableChangeMap);
            }
          });
    } catch (SQLException e) {
      throw BitSailException.asBitSailException(BinlogReaderErrorCode.SQL_ERROR,
          String.format("Failed to read schema for table %s by running %s", tableId, sql),
          e);
    }
  }

  private void parseSchemaByDdl(
      String ddl, TableId tableId, Map<TableId, TableChanges.TableChange> tableChangeMap) {
    final MySqlOffsetContext offsetContext = MySqlOffsetContext.initial(connectorConfig);
    List<SchemaChangeEvent> schemaChangeEvents =
        databaseSchema.parseSnapshotDdl(
            ddl, tableId.catalog(), offsetContext, Instant.now());
    for (SchemaChangeEvent schemaChangeEvent : schemaChangeEvents) {
      for (TableChanges.TableChange tableChange : schemaChangeEvent.getTableChanges()) {
        tableChangeMap.put(tableId, tableChange);
      }
    }
  }

  private void buildSchemaByDescTable(
      JdbcConnection jdbc,
      String descTable,
      TableId tableId,
      Map<TableId, TableChanges.TableChange> tableChangeMap) {
    List<MysqlFieldDescriber> fieldMetas = new ArrayList<>();
    List<String> primaryKeys = new ArrayList<>();
    try {
      jdbc.query(
          descTable,
          rs -> {
            while (rs.next()) {
              MysqlFieldDescriber fieldDescriber = new MysqlFieldDescriber();
              fieldDescriber.setColumnName(rs.getString("Field"));
              fieldDescriber.setColumnType(rs.getString("Type"));
              fieldDescriber.setNullable(
                  StringUtils.equalsIgnoreCase(rs.getString("Null"), "YES"));
              fieldDescriber.setKey("PRI".equalsIgnoreCase(rs.getString("Key")));
              fieldDescriber.setUnique("UNI".equalsIgnoreCase(rs.getString("Key")));
              fieldDescriber.setDefaultValue(rs.getString("Default"));
              fieldDescriber.setExtra(rs.getString("Extra"));
              if (fieldDescriber.isKey()) {
                primaryKeys.add(fieldDescriber.getColumnName());
              }
              fieldMetas.add(fieldDescriber);
            }
          });
      parseSchemaByDdl(
          new MysqlTableDescriber(quoteTableId(tableId), fieldMetas, primaryKeys).toDdl(),
          tableId,
          tableChangeMap);
    } catch (SQLException e) {
      throw BitSailException.asBitSailException(BinlogReaderErrorCode.SQL_ERROR,
          String.format(
              "Failed to read schema for table %s by running %s", tableId, descTable),
          e);
    }
  }
}
