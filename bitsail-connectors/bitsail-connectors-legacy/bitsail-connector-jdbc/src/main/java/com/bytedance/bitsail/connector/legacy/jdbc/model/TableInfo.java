/*
 * Copyright 2022-present ByteDance.
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

package com.bytedance.bitsail.connector.legacy.jdbc.model;

import com.bytedance.bitsail.common.model.ColumnInfo;

import java.util.List;

/**
 * @desc:
 */
public class TableInfo {
  private String dbName;
  private String tableName;
  private List<ColumnInfo> columnInfoList;

  public TableInfo(String dbName, String tableName, List<ColumnInfo> columnInfoList) {
    this.dbName = dbName;
    this.tableName = tableName;
    this.columnInfoList = columnInfoList;
  }

  public String getDbName() {
    return dbName;
  }

  public List<ColumnInfo> getColumnInfoList() {
    return columnInfoList;
  }

  public String getTableName() {
    return tableName;
  }
}
