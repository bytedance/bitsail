/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.bytedance.bitsail.connector.legacy.hive.common;

import com.bytedance.bitsail.common.catalog.table.CatalogTable;
import com.bytedance.bitsail.common.catalog.table.CatalogTableColumn;
import com.bytedance.bitsail.common.catalog.table.CatalogTableDefinition;
import com.bytedance.bitsail.common.catalog.table.CatalogTableSchema;
import com.bytedance.bitsail.common.catalog.table.TableCatalog;
import com.bytedance.bitsail.common.catalog.table.TableOperation;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.exception.FrameworkErrorCode;
import com.bytedance.bitsail.common.model.ColumnInfo;
import com.bytedance.bitsail.common.type.TypeInfoConverter;
import com.bytedance.bitsail.common.typeinfo.TypeInfo;
import com.bytedance.bitsail.connector.legacy.hive.option.HiveReaderOptions;

import com.bytedance.bitsail.shaded.hive.client.HiveMetaClientUtil;

import org.apache.hadoop.hive.conf.HiveConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;

public class HiveTableCatalog implements TableCatalog {
  private static final Logger LOG = LoggerFactory.getLogger(HiveTableCatalog.class);

  private final String database;
  private final String table;
  private final HiveConf hiveConf;

  private CatalogTableDefinition catalogTableDefinition;
  private TypeInfoConverter typeInfoConverter;

  public HiveTableCatalog(BitSailConfiguration commonConfiguration,
                          BitSailConfiguration readerConfiguration,
                          HiveConf hiveConf) {
    this.database = readerConfiguration
        .getNecessaryOption(HiveReaderOptions.DB_NAME, FrameworkErrorCode.REQUIRED_VALUE);
    this.table = readerConfiguration
        .getNecessaryOption(HiveReaderOptions.TABLE_NAME, FrameworkErrorCode.REQUIRED_VALUE);
    this.hiveConf = hiveConf;
  }

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
  public CatalogTableDefinition referenceTable() {
    if (Objects.isNull(catalogTableDefinition)) {
      catalogTableDefinition = CatalogTableDefinition
          .builder()
          .database(database)
          .table(table)
          .build();

    }
    return catalogTableDefinition;
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
  public void createTable(CatalogTable catalogTable) {
    //todo
  }

  @Override
  public void alterTable(TableOperation tableOperation, CatalogTable table) {
    //todo
  }

  @Override
  public void alterTableColumns(TableOperation tableOperation,
                                List<CatalogTableColumn> catalogTableColumns) {
    //todo
  }

  @Override
  public boolean compareTypeCompatible(TypeInfo<?> original, TypeInfo<?> compared) {
    return original.getTypeClass() == compared.getTypeClass();
  }

  private CatalogTableSchema getCatalogTableSchema(List<ColumnInfo> columnInfos) {
    List<CatalogTableColumn> tableColumns = convertTableColumn(typeInfoConverter, columnInfos);
    return CatalogTableSchema.builder()
        .columns(tableColumns)
        .primaryKeys(null)
        .build();
  }
}
