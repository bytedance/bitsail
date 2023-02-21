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

package com.bytedance.bitsail.common.catalog;

import com.bytedance.bitsail.common.catalog.table.CatalogTable;
import com.bytedance.bitsail.common.catalog.table.CatalogTableColumn;
import com.bytedance.bitsail.common.catalog.table.CatalogTableSchema;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.io.Serializable;
import java.util.List;
import java.util.Set;

public enum TableCatalogStrategy implements Serializable {

  DISABLE,

  INTERSECT {
    @Override
    public CatalogTableSchema apply(CatalogTable reader, CatalogTable writer) {
      CatalogTableSchema readerCatalogTableSchema = reader.getCatalogTableSchema();
      CatalogTableSchema writerCatalogTableSchema = writer.getCatalogTableSchema();

      List<CatalogTableColumn> readerColumns = readerCatalogTableSchema.getColumns();
      List<CatalogTableColumn> writerColumns = writerCatalogTableSchema.getColumns();

      Set<CatalogTableColumn> intersect = Sets.intersection(
              Sets.newHashSet(readerColumns),
              Sets.newHashSet(writerColumns))
          .copyInto(Sets.newHashSet());

      return new CatalogTableSchema(Lists.newArrayList(intersect.iterator()));
    }
  },

  SOURCE_ONLY {
    @Override
    public CatalogTableSchema apply(CatalogTable reader, CatalogTable writer) {
      return reader.getCatalogTableSchema();
    }
  };

  public CatalogTableSchema apply(CatalogTable reader,
                                  CatalogTable writer) {
    throw new UnsupportedOperationException();
  }

}
