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

package com.bytedance.bitsail.common.catalog.table;

import lombok.Builder;
import lombok.Getter;

import java.io.Serializable;

@Getter
@Builder
public class CatalogTable implements Serializable {

  private CatalogTableDefinition catalogTableDefinition;

  private final CatalogTableSchema catalogTableSchema;

  private final String comment;

  public CatalogTable(CatalogTableDefinition catalogTableDefinition,
                      CatalogTableSchema catalogTableSchema) {
    this(catalogTableDefinition, catalogTableSchema, null);
  }

  public CatalogTable(CatalogTableDefinition catalogTableDefinition,
                      CatalogTableSchema catalogTableSchema,
                      String comment) {
    this.catalogTableDefinition = catalogTableDefinition;
    this.catalogTableSchema = catalogTableSchema;
    this.comment = comment;
  }

  @Override
  public String toString() {
    return "CatalogTable{" +
        "catalogTableDefinition=" + catalogTableDefinition +
        ", catalogTableSchema=" + catalogTableSchema +
        ", comment='" + comment + '\'' +
        '}';
  }
}
