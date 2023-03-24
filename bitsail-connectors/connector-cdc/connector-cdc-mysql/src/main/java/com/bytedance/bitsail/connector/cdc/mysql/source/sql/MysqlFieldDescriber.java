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

import lombok.Data;

/**
 * Describe a Mysql Field, and convert this field to DDL.
 */
@Data
public class MysqlFieldDescriber {
  private String columnName;
  private String columnType;
  private boolean nullable;
  private boolean key;
  private String defaultValue;
  private String extra;
  private boolean unique;

  public static String quote(String dbOrTableName) {
    return "`" + dbOrTableName + "`";
  }

  public String toDdl() {
    return quote(columnName) + " " + columnType + " " + (nullable ? "" : "NOT NULL");
  }
}
