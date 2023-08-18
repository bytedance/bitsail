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

package com.bytedance.bitsail.connector.elasticsearch.catalog;


import com.bytedance.bitsail.common.catalog.table.CatalogTable;
import com.bytedance.bitsail.common.catalog.table.CatalogTableColumn;
import com.bytedance.bitsail.common.catalog.table.CatalogTableDefinition;
import com.bytedance.bitsail.common.catalog.table.TableCatalog;
import com.bytedance.bitsail.common.catalog.table.TableOperation;
import com.bytedance.bitsail.common.type.TypeInfoConverter;
import com.bytedance.bitsail.common.typeinfo.TypeInfo;

import java.util.List;

public class ElasticsearchCatalog implements TableCatalog {
  @Override
  public void open(TypeInfoConverter typeInfoConverter) {
    
  }

  @Override
  public void close() {

  }

  @Override
  public CatalogTableDefinition createCatalogTableDefinition() {
    return null;
  }

  @Override
  public boolean tableExists(CatalogTableDefinition catalogTableDefinition) {
    return false;
  }

  @Override
  public CatalogTable getCatalogTable(CatalogTableDefinition catalogTableDefinition) {
    return null;
  }

  @Override
  public void createTable(CatalogTableDefinition catalogTableDefinition, CatalogTable catalogTable) {

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
