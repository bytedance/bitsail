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

import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.catalog.table.CatalogTable;
import com.bytedance.bitsail.common.catalog.table.CatalogTableColumn;
import com.bytedance.bitsail.common.catalog.table.CatalogTableDefinition;
import com.bytedance.bitsail.common.catalog.table.CatalogTableSchema;
import com.bytedance.bitsail.common.catalog.table.TableCatalog;
import com.bytedance.bitsail.common.catalog.table.TableOperation;
import com.bytedance.bitsail.common.type.TypeInfoConverter;
import com.bytedance.bitsail.common.typeinfo.TypeInfo;
import com.bytedance.bitsail.connector.legacy.jdbc.exception.JDBCPluginErrorCode;
import com.bytedance.bitsail.connector.legacy.jdbc.model.TableInfo;
import com.bytedance.bitsail.connector.legacy.jdbc.utils.MysqlUtil;

import lombok.Builder;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class MySQLTableCatalog implements TableCatalog {
  private static final Logger LOG = LoggerFactory.getLogger(MySQLTableCatalog.class);

  private final String database;
  private final String table;
  private final String schema;
  private final String username;
  private final String password;
  private final String url;
  private final String customizedSQL;
  private final boolean useCustomizedSQL;
  private TypeInfoConverter typeInfoConverter;

  @Builder
  public MySQLTableCatalog(String database,
                           String table,
                           String schema,
                           String username,
                           String password,
                           String url,
                           String customizedSQL) {
    this.database = database;
    this.table = table;
    this.schema = schema;
    this.username = username;
    this.password = password;
    this.url = url;
    this.customizedSQL = customizedSQL;
    this.useCustomizedSQL = StringUtils.isNotEmpty(customizedSQL);
  }

  @Override
  public void open(TypeInfoConverter typeInfoConverter) {
    this.typeInfoConverter = typeInfoConverter;
  }

  @Override
  public void close() {
    //ignore
  }

  @Override
  public CatalogTableDefinition createCatalogTableDefinition() {
    return CatalogTableDefinition
        .builder()
        .database(database)
        .table(table)
        .build();
  }

  @Override
  public boolean tableExists(CatalogTableDefinition catalogTableDefinition) {
    //todo doesn't check
    return true;
  }

  @Override
  public CatalogTable getCatalogTable(CatalogTableDefinition catalogTableDefinition) {
    TableInfo tableInfo;
    try {
      if (useCustomizedSQL) {
        tableInfo = MysqlUtil.getInstance()
            .getCustomizedSQLTableInfo(url, username, password, database, customizedSQL);
      } else {
        tableInfo = MysqlUtil.getInstance()
            .getTableInfo(url, username, password, database, schema, table, null, null);
      }
    } catch (Exception e) {
      LOG.error("Failed to get table info by the definition {}.", catalogTableDefinition);
      throw BitSailException.asBitSailException(JDBCPluginErrorCode.INTERNAL_ERROR, e);
    }
    return CatalogTable
        .builder()
        .catalogTableDefinition(catalogTableDefinition)
        .catalogTableSchema(CatalogTableSchema.builder()
            .columns(convertTableColumn(
                typeInfoConverter,
                tableInfo.getColumnInfoList())
            ).build()
        ).build();
  }

  @Override
  public void createTable(CatalogTableDefinition catalogTableDefinition, CatalogTable catalogTable) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alterTable(TableOperation tableOperation,
                         CatalogTable table) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alterTableColumns(TableOperation tableOperation,
                                List<CatalogTableColumn> catalogTableColumns) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean compareTypeCompatible(TypeInfo<?> original,
                                       TypeInfo<?> compared) {
    return original.getTypeClass() == compared.getTypeClass();
  }
}
