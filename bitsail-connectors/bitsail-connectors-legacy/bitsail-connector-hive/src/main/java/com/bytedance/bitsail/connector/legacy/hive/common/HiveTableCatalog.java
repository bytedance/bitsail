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

package com.bytedance.bitsail.connector.legacy.hive.common;

import com.bytedance.bitsail.common.catalog.table.CatalogTable;
import com.bytedance.bitsail.common.catalog.table.CatalogTableColumn;
import com.bytedance.bitsail.common.catalog.table.CatalogTableDefinition;
import com.bytedance.bitsail.common.catalog.table.CatalogTableSchema;
import com.bytedance.bitsail.common.catalog.table.TableCatalog;
import com.bytedance.bitsail.common.catalog.table.TableOperation;
import com.bytedance.bitsail.common.model.ColumnInfo;
import com.bytedance.bitsail.common.type.TypeInfoConverter;
import com.bytedance.bitsail.common.typeinfo.TypeInfo;

import com.bytedance.bitsail.shaded.hive.client.HiveMetaClientUtil;

import lombok.Builder;
import org.apache.hadoop.hive.conf.HiveConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

@Builder
public class HiveTableCatalog implements TableCatalog {
  private static final Logger LOG = LoggerFactory.getLogger(HiveTableCatalog.class);

  private final String namespace;
  private final String database;
  private final String table;
  private final HiveConf hiveConf;

  private TypeInfoConverter typeInfoConverter;

  @Override
  public void open(TypeInfoConverter typeInfoConverter) {
    this.typeInfoConverter = typeInfoConverter;
    //ignore
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
    //todo real check.
    return true;
  }

  @Override
  public CatalogTable getCatalogTable(CatalogTableDefinition catalogTableDefinition) {
    try {
      List<ColumnInfo> columnInfo = HiveMetaClientUtil.getColumnInfo(hiveConf,
          catalogTableDefinition.getDatabase(),
          catalogTableDefinition.getTable());

      return CatalogTable.builder()
          .catalogTableDefinition(catalogTableDefinition)
          .catalogTableSchema(getCatalogTableSchema(columnInfo))
          .build();
    } catch (Exception e) {
      LOG.error("Acquire hive catalog table failed", e);
      throw new IllegalStateException();
    }
  }

  @Override
  public void createTable(CatalogTableDefinition catalogTableDefinition, CatalogTable catalogTable) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alterTable(TableOperation tableOperation, CatalogTable table) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alterTableColumns(TableOperation tableOperation,
                                List<CatalogTableColumn> catalogTableColumns) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean compareTypeCompatible(TypeInfo<?> original, TypeInfo<?> compared) {
    return original.getTypeClass() == compared.getTypeClass();
  }

  private CatalogTableSchema getCatalogTableSchema(List<ColumnInfo> columnInfos) {
    List<CatalogTableColumn> tableColumns = convertTableColumn(typeInfoConverter, columnInfos);
    return CatalogTableSchema.builder()
        .columns(tableColumns)
        .build();
  }
}
