/*
 *       Copyright 2022-2023 Bytedance Ltd. and/or its affiliates.
 *
 *       Licensed under the Apache License, Version 2.0 (the "License");
 *       you may not use this file except in compliance with the License.
 *       You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *       Unless required by applicable law or agreed to in writing, software
 *       distributed under the License is distributed on an "AS IS" BASIS,
 *       WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *       See the License for the specific language governing permissions and
 *       limitations under the License.
 */

package com.bytedance.bitsail.core.common.catalog;

import com.bytedance.bitsail.common.catalog.table.Catalog;
import com.bytedance.bitsail.common.catalog.table.CatalogTable;
import com.bytedance.bitsail.common.catalog.table.CatalogTableColumn;
import com.bytedance.bitsail.common.catalog.table.TableId;
import com.bytedance.bitsail.common.catalog.table.TableOperation;
import com.bytedance.bitsail.common.type.TypeInfoConverter;
import com.bytedance.bitsail.common.typeinfo.TypeInfo;

import java.util.List;

public class FakeCatalog implements Catalog {

  private final List<TableId> tableIds;

  private final List<CatalogTable> catalogTables;

  public FakeCatalog(List<TableId> tableIds, List<CatalogTable> catalogTables) {
    this.tableIds = tableIds;
    this.catalogTables = catalogTables;
  }

  @Override
  public void open(TypeInfoConverter typeInfoConverter) {

  }

  @Override
  public void close() {

  }

  @Override
  public TableId createCatalogTableDefinition() {
    return null;
  }

  @Override
  public List<TableId> listTables() {
    return tableIds;
  }

  @Override
  public boolean tableExists(TableId catalogTableDefinition) {
    return false;
  }

  @Override
  public CatalogTable getCatalogTable(TableId tableId) {
    return catalogTables.get(catalogTables.indexOf(tableId));
  }

  @Override
  public void createTable(TableId catalogTableDefinition, CatalogTable catalogTable) {

  }

  @Override
  public void alterTable(TableOperation tableOperation, CatalogTable table) {

  }

  @Override
  public void alterTableColumns(TableOperation tableOperation, List<CatalogTableColumn> catalogTableColumns) {

  }

  @Override
  public boolean compareTypeCompatible(TypeInfo<?> original, TypeInfo<?> compared) {
    return false;
  }
}
