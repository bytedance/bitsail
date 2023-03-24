/*
 * Copyright 2022-2023 Bytedance Ltd. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.bytedance.bitsail.connector.cdc.mysql.source.sql;

import lombok.AllArgsConstructor;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Describe a Mysql table, and convert this table to a CREATE TABLE DDL.
 */
@AllArgsConstructor
public class MysqlTableDescriber {
  String tableName;
  List<MysqlFieldDescriber> fieldDefinitions;
  List<String> primaryKeys;

  public String toDdl() {
    return String.format(
        "CREATE TABLE %s (\n\t %s %s );",
        tableName, fieldDefinitions(), pkDefinition());
  }

  private String fieldDefinitions() {
    return fieldDefinitions.stream()
        .map(MysqlFieldDescriber::toDdl)
        .collect(Collectors.joining(", \n\t"));
  }

  private String pkDefinition() {
    StringBuilder pkDefinition = new StringBuilder();
    if (primaryKeys != null && !primaryKeys.isEmpty()) {
      pkDefinition.append(",");
      pkDefinition.append(
          String.format(
              "PRIMARY KEY ( %s )",
              primaryKeys.stream()
                  .map(MysqlFieldDescriber::quote)
                  .collect(Collectors.joining(","))));
    }
    return pkDefinition.toString();
  }
}
