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

package com.bytedance.bitsail.common.catalog;

import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.catalog.table.CatalogTable;
import com.bytedance.bitsail.common.catalog.table.CatalogTableAlterDefinition;
import com.bytedance.bitsail.common.catalog.table.CatalogTableColumn;
import com.bytedance.bitsail.common.catalog.table.CatalogTableDefinition;
import com.bytedance.bitsail.common.catalog.table.CatalogTableSchema;
import com.bytedance.bitsail.common.catalog.table.TableCatalog;
import com.bytedance.bitsail.common.catalog.table.TableOperation;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.model.ColumnInfo;
import com.bytedance.bitsail.common.option.ReaderOptions;
import com.bytedance.bitsail.common.option.WriterOptions;
import com.bytedance.bitsail.common.type.TypeInfoConverter;
import com.bytedance.bitsail.common.typeinfo.TypeInfo;

import lombok.Builder;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.compress.utils.Lists;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class TableCatalogManager {

  private static final Logger LOG = LoggerFactory.getLogger(TableCatalogManager.class);

  private final TypeInfoConverter readerTypeInfoConverter;
  private final TypeInfoConverter writerTypeInfoConverter;

  private final TableCatalog readerTableCatalog;
  private final TableCatalog writerTableCatalog;

  private final BitSailConfiguration commonConfiguration;
  private final BitSailConfiguration readerConfiguration;
  private final BitSailConfiguration writerConfiguration;

  private TableCatalogStrategy tableCatalogStrategy;
  private boolean tableCatalogSync;
  private boolean tableCatalogAddSync;
  private boolean tableCatalogDeleteSync;
  private boolean tableCatalogUpdateSync;

  private CatalogTable readerCatalogTable;
  private CatalogTable writerCatalogTable;

  private List<CatalogTableColumn> finalCatalogColumns;

  @Builder
  public TableCatalogManager(TypeInfoConverter readerTypeInfoConverter,
                             TypeInfoConverter writerTypeInfoConverter,
                             TableCatalog readerTableCatalog,
                             TableCatalog writerTableCatalog,
                             BitSailConfiguration commonConfiguration,
                             BitSailConfiguration readerConfiguration,
                             BitSailConfiguration writerConfiguration) {
    this.readerTypeInfoConverter = readerTypeInfoConverter;
    this.writerTypeInfoConverter = writerTypeInfoConverter;
    this.readerTableCatalog = readerTableCatalog;
    this.writerTableCatalog = writerTableCatalog;
    this.commonConfiguration = commonConfiguration;
    this.readerConfiguration = readerConfiguration;
    this.writerConfiguration = writerConfiguration;

    prepareCatalogManager();
  }

  private void prepareCatalogManager() {
    tableCatalogStrategy =
        TableCatalogStrategy.valueOf(StringUtils.upperCase(commonConfiguration
            .get(TableCatalogOptions.COLUMN_ALIGN_STRATEGY)));
    this.tableCatalogSync = commonConfiguration.get(TableCatalogOptions.SYNC_DDL);
    this.tableCatalogAddSync = commonConfiguration.get(TableCatalogOptions.SYNC_DDL_IGNORE_ADD);
    this.tableCatalogDeleteSync = commonConfiguration.get(TableCatalogOptions.SYNC_DDL_IGNORE_DROP);
    this.tableCatalogUpdateSync = commonConfiguration.get(TableCatalogOptions.SYNC_DDL_IGNORE_UPDATE);
  }

  public void alignmentCatalogTable() throws Exception {
    if (Objects.isNull(readerTableCatalog) || Objects.isNull(writerTableCatalog)) {
      return;
    }

    if (TableCatalogStrategy.DISABLE.equals(tableCatalogStrategy)) {
      LOG.warn("Ignore table catalog alignment.");
      return;
    }
    //start table column catalog
    startTableCatalog();

    CatalogTableDefinition readerTableDefinition = readerTableCatalog.createCatalogTableDefinition();
    CatalogTableDefinition writerTableDefinition = readerTableCatalog.createCatalogTableDefinition();
    if (!readerTableCatalog.tableExists(readerTableDefinition)) {
      throw BitSailException.asBitSailException(TableCatalogErrorCode.TABLE_CATALOG_TABLE_NOT_EXISTS,
          String.format("Reader table definition %s not exists.", readerTableDefinition));
    }
    readerCatalogTable = readerTableCatalog.getCatalogTable(readerTableDefinition);

    if (!writerTableCatalog.tableExists(writerTableDefinition)) {
      writerTableCatalog.createTable(readerCatalogTable);
      throw BitSailException.asBitSailException(TableCatalogErrorCode.TABLE_CATALOG_TABLE_NOT_EXISTS,
          String.format("Writer table definition %s not exists.", writerTableDefinition));

    }
    writerCatalogTable = writerTableCatalog.getCatalogTable(writerTableDefinition);
    CatalogTableSchema catalogTableSchema = tableCatalogStrategy
        .apply(readerCatalogTable, writerCatalogTable);

    LOG.info("Base catalog table schema {}.", catalogTableSchema);

    if (tableCatalogSync) {
      CatalogTableAlterDefinition catalogTableAlterDefinition =
          calNecessaryCatalogSchema(catalogTableSchema);
      alterCatalogSchema(catalogTableAlterDefinition);
    } else {
      finalCatalogColumns.addAll(catalogTableSchema.getColumns());
    }

    List<ColumnInfo> finalReaderColumnInfos = transform(finalCatalogColumns, readerTypeInfoConverter);
    List<ColumnInfo> finalWriterColumnInfos = transform(finalCatalogColumns, writerTypeInfoConverter);

    readerConfiguration.set(ReaderOptions.BaseReaderOptions.COLUMNS, finalReaderColumnInfos);
    LOG.info("Final reader's columns: {}", finalReaderColumnInfos);

    writerConfiguration.set(WriterOptions.BaseWriterOptions.COLUMNS, finalWriterColumnInfos);
    LOG.info("Final writer's columns: {}", finalWriterColumnInfos);

    closeTableCatalog();
  }

  private List<ColumnInfo> transform(List<CatalogTableColumn> catalogTableColumns,
                                     TypeInfoConverter typeInfoConverter) {
    List<ColumnInfo> columnInfos = Lists.newArrayList();
    for (CatalogTableColumn catalogTableColumn : catalogTableColumns) {
      columnInfos.add(ColumnInfo
          .builder()
          .name(catalogTableColumn.getName())
          .type(typeInfoConverter.fromTypeInfo(catalogTableColumn.getType()))
          .build());
    }
    return columnInfos;
  }

  private void alterCatalogSchema(CatalogTableAlterDefinition catalogTableAlterDefinition) {
    if (!catalogTableAlterDefinition.isNotEmpty()) {
      return;
    }
    if (!tableCatalogAddSync &&
        CollectionUtils.isNotEmpty(catalogTableAlterDefinition.getPendingAddColumns())) {
      writerTableCatalog.alterTableColumns(
          TableOperation.ALTER_COLUMNS_ADD,
          catalogTableAlterDefinition.getPendingAddColumns()
      );
    }

    if (!tableCatalogDeleteSync &&
        CollectionUtils.isNotEmpty(catalogTableAlterDefinition.getPendingDeleteColumns())) {
      writerTableCatalog.alterTableColumns(
          TableOperation.ALTER_COLUMNS_DELETE,
          catalogTableAlterDefinition.getPendingDeleteColumns()
      );
    }

    if (!tableCatalogUpdateSync &&
        CollectionUtils.isNotEmpty(catalogTableAlterDefinition.getPendingUpdateColumns())) {
      writerTableCatalog.alterTableColumns(
          TableOperation.ALTER_COLUMNS_UPDATE,
          catalogTableAlterDefinition.getPendingUpdateColumns()
      );
    }
  }

  private CatalogTableAlterDefinition calNecessaryCatalogSchema(CatalogTableSchema baseCatalogTableSchema) {
    CatalogTableSchema writeCatalogTableSchema = writerCatalogTable.getCatalogTableSchema();

    Map<String, TypeInfo<?>> writeTableColumnMapping = writeCatalogTableSchema
        .getColumns()
        .stream()
        .collect(Collectors.toMap(CatalogTableColumn::getName, CatalogTableColumn::getType));

    List<String> writePrimaryTableColumnMapping = Lists.newArrayList();
    if (CollectionUtils.isNotEmpty(writeCatalogTableSchema.getPrimaryKeys())) {
      writePrimaryTableColumnMapping.addAll(writeCatalogTableSchema
          .getPrimaryKeys()
          .stream()
          .map(column -> StringUtils.lowerCase(column.getName()))
          .collect(Collectors.toList()));
    }

    finalCatalogColumns = Lists.newArrayList();

    List<CatalogTableColumn> pendingAddTableColumns = Lists.newArrayList();
    List<CatalogTableColumn> pendingUpdateTableColumns = Lists.newArrayList();
    List<CatalogTableColumn> pendingDeleteTableColumns = Lists.newArrayList();
    for (CatalogTableColumn catalogTableColumn : baseCatalogTableSchema.getColumns()) {
      String baseCatalogColumnName = StringUtils.lowerCase(catalogTableColumn.getName());
      if (writePrimaryTableColumnMapping.contains(baseCatalogColumnName)) {
        finalCatalogColumns.add(catalogTableColumn);
        continue;
      }

      if (writeTableColumnMapping.containsKey(baseCatalogColumnName)) {
        TypeInfo<?> writerTypeInfo = writeTableColumnMapping.get(baseCatalogColumnName);
        TypeInfo<?> baseTypeInfo = catalogTableColumn.getType();

        finalCatalogColumns.add(catalogTableColumn);
        if (!writerTableCatalog.compareTypeCompatible(writerTypeInfo, baseTypeInfo)) {
          pendingUpdateTableColumns.add(catalogTableColumn);
        }
      } else {
        finalCatalogColumns.add(catalogTableColumn);
        pendingAddTableColumns.add(catalogTableColumn);
      }
    }

    return CatalogTableAlterDefinition.builder()
        .pendingDeleteColumns(pendingDeleteTableColumns)
        .pendingAddColumns(pendingAddTableColumns)
        .pendingUpdateColumns(pendingUpdateTableColumns)
        .build();
  }

  private void startTableCatalog() {
    readerTableCatalog.open(readerTypeInfoConverter);
    writerTableCatalog.open(writerTypeInfoConverter);
  }

  private void closeTableCatalog() {
    readerTableCatalog.close();
    writerTableCatalog.close();
  }

}
