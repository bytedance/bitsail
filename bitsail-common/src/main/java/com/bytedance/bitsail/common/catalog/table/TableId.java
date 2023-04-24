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
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.Objects;

@Getter
@Builder
public class TableId implements Serializable {
  private static final int DEFAULT_TABLE_ID_LENGTH = 2;
  private static final int CONTAINS_SCHEMA_TABLE_ID_LENGTH = 3;

  private final String database;
  private final String schema;
  private final String table;

  public TableId(String database, String schema, String table) {
    this.database = database;
    this.schema = schema;
    this.table = table;
  }

  public static TableId of(String tableId) {
    String[] paths = tableId.split("\\.");

    if (paths.length == DEFAULT_TABLE_ID_LENGTH) {
      return of(paths[0], paths[1]);
    }
    if (paths.length == CONTAINS_SCHEMA_TABLE_ID_LENGTH) {
      return of(paths[0], paths[1], paths[2]);
    }
    throw new IllegalArgumentException(
        String.format("Can't get table-id from value: %s", tableId));
  }

  public static TableId of(String database, String table) {
    return of(database, null, table);
  }

  public static TableId of(String database, String schema, String table) {
    return new TableId(database, schema, table);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TableId tableId = (TableId) o;
    return Objects.equals(database, tableId.database) && Objects.equals(schema, tableId.schema) && Objects.equals(table, tableId.table);
  }

  @Override
  public int hashCode() {
    return Objects.hash(database, schema, table);
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append(database)
        .append(".");

    if (StringUtils.isNotEmpty(schema)) {
      builder.append(schema)
          .append(".");
    }

    builder.append(table);
    return builder.toString();
  }
}
