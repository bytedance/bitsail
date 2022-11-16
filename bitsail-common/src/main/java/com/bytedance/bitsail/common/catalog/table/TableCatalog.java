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

package com.bytedance.bitsail.common.catalog.table;

import com.bytedance.bitsail.common.type.TypeInfoConverter;
import com.bytedance.bitsail.common.typeinfo.TypeInfo;

import java.io.Serializable;
import java.util.List;

/**
 * Table catalog only for the signal table for now.
 */
public interface TableCatalog extends Serializable {

  /**
   * Open Table catalog
   */
  void open(TypeInfoConverter typeInfoConverter);

  /**
   * Close table catalog
   */
  void close();

  /**
   * Get the reference table for the table catalog.
   */
  CatalogTableDefinition referenceTable();

  /**
   * Check the table exits or not.
   */
  boolean tableExists(CatalogTableDefinition catalogTableDefinition);

  /**
   * Acquire catalog table by the table definition.
   */
  CatalogTable getCatalogTable(CatalogTableDefinition catalogTableDefinition);

  /**
   * Create table
   */
  void createTable(CatalogTable catalogTable);

  /**
   * Alter table
   */
  void alterTable(TableOperation tableOperation,
                  CatalogTable table);

  /**
   * Alter table columns.
   */
  void alterTableColumns(TableOperation tableOperation,
                         List<CatalogTableColumn> catalogTableColumns);

  boolean compareTypeCompatible(TypeInfo<?> original, TypeInfo<?> compared);

}
