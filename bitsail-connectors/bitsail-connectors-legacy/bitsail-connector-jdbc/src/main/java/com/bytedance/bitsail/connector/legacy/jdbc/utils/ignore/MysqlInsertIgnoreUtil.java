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

package com.bytedance.bitsail.connector.legacy.jdbc.utils.ignore;

import com.bytedance.bitsail.connector.legacy.jdbc.sink.JDBCOutputFormat;

import java.util.List;
import java.util.stream.Collectors;

public class MysqlInsertIgnoreUtil extends JDBCInsertIgnoreUtil {
  public MysqlInsertIgnoreUtil(JDBCOutputFormat jdbcOutputFormat, String[] shardKeys) {
    super(jdbcOutputFormat, shardKeys);
  }

  @Override
  public String genInsertIgnoreTemplate(String table, List<String> columns) {
    return String.format("INSERT IGNORE INTO %s (%s) VALUES (%s)",
        table,
        columns.stream().map(this::getQuoteColumn).collect(Collectors.joining(",")),
        columns.stream().map(col -> "?").collect(Collectors.joining(","))
    );
  }

  protected String getQuoteColumn(String col) {
    return this.jdbcOutputFormat.getQuoteColumn(col);
  }

}
