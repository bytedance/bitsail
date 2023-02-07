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

package com.bytedance.bitsail.common.catalog.fake;

import com.bytedance.bitsail.common.catalog.table.CatalogTable;
import com.bytedance.bitsail.common.catalog.table.CatalogTableColumn;
import com.bytedance.bitsail.common.catalog.table.CatalogTableDefinition;
import com.bytedance.bitsail.common.catalog.table.CatalogTableSchema;
import com.bytedance.bitsail.common.catalog.table.TableCatalog;
import com.bytedance.bitsail.common.catalog.table.TableOperation;
import com.bytedance.bitsail.common.model.ColumnInfo;
import com.bytedance.bitsail.common.type.BitSailTypeInfoConverter;
import com.bytedance.bitsail.common.type.TypeInfoConverter;
import com.bytedance.bitsail.common.typeinfo.TypeInfo;

import com.google.common.collect.Lists;
import lombok.Getter;

import java.util.List;
import java.util.stream.Collectors;

public class FakeTableCatalog implements TableCatalog {

  private final List<ColumnInfo> columnInfos;

  private final CatalogTableDefinition tableDefinition;

  private final TypeInfoConverter typeInfoConverter;

  private final List<CatalogTableColumn> catalogTableColumns;

  @Getter
  private final List<CatalogTableColumn> addedTableColumns;
  @Getter
  private final List<CatalogTableColumn> updatedTableColumns;
  @Getter
  private final List<CatalogTableColumn> deletedTableColumns;

  public FakeTableCatalog(List<ColumnInfo> columnInfos,
                          CatalogTableDefinition tableDefinition) {
    this.columnInfos = columnInfos;
    this.tableDefinition = tableDefinition;
    this.typeInfoConverter = new BitSailTypeInfoConverter();
    this.catalogTableColumns = columnInfos.stream()
        .map(column ->
            CatalogTableColumn.builder()
                .name(column.getName())
                .type(typeInfoConverter.fromTypeString(column.getType()))
                .build()
        ).collect(Collectors.toList());
    addedTableColumns = Lists.newArrayList();
    updatedTableColumns = Lists.newArrayList();
    deletedTableColumns = Lists.newArrayList();
  }

  @Override
  public void open(TypeInfoConverter typeInfoConverter) {

  }

  @Override
  public void close() {

  }

  @Override
  public CatalogTableDefinition createCatalogTableDefinition() {
    return tableDefinition;
  }

  @Override
  public boolean tableExists(CatalogTableDefinition catalogTableDefinition) {
    return true;
  }

  @Override
  public CatalogTable getCatalogTable(CatalogTableDefinition catalogTableDefinition) {
    CatalogTableSchema tableSchema = CatalogTableSchema.builder()
        .columns(catalogTableColumns)
        .primaryKeys(null)
        .build();
    return CatalogTable.builder()
        .catalogTableDefinition(catalogTableDefinition)
        .catalogTableSchema(tableSchema)
        .build();
  }

  @Override
  public void createTable(CatalogTableDefinition catalogTableDefinition, CatalogTable catalogTable) {

  }

  @Override
  public void alterTable(TableOperation tableOperation, CatalogTable table) {

  }

  @Override
  public void alterTableColumns(TableOperation tableOperation, List<CatalogTableColumn> catalogTableColumns) {
    if (TableOperation.ALTER_COLUMNS_ADD.equals(tableOperation)) {
      addedTableColumns.addAll(catalogTableColumns);
    }
    if (TableOperation.ALTER_COLUMNS_UPDATE.equals(tableOperation)) {
      updatedTableColumns.addAll(catalogTableColumns);
    }
    if (TableOperation.ALTER_COLUMNS_DELETE.equals(tableOperation)) {
      deletedTableColumns.addAll(catalogTableColumns);
    }
  }

  @Override
  public boolean compareTypeCompatible(TypeInfo<?> original, TypeInfo<?> compared) {
    return original.getTypeClass() == compared.getTypeClass();
  }

  @Override
  public List<CatalogTableColumn> convertTableColumn(TypeInfoConverter typeInfoConverter, List<ColumnInfo> columnInfos) {
    return TableCatalog.super.convertTableColumn(typeInfoConverter, columnInfos);
  }
}
