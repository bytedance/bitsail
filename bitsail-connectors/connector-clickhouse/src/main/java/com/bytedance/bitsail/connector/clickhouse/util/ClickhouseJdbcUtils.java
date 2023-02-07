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

package com.bytedance.bitsail.connector.clickhouse.util;

import com.bytedance.bitsail.common.model.ColumnInfo;
import com.bytedance.bitsail.connector.clickhouse.source.split.ClickhouseSourceSplit;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ClickhouseJdbcUtils {
  private static final Logger LOG = LoggerFactory.getLogger(ClickhouseJdbcUtils.class);

  private static final String QUOTE_IDENTIFIER = "`";

  private static final String SELECT_TEMPLATE = "SELECT %s FROM %s";
  private static final String SELECT_MAX_TEMPLATE = "SELECT MAX(%s) FROM %s";
  private static final String SELECT_MIN_TEMPLATE = "SELECT MIN(%s) FROM %s";
  private static final String SELECT_MIN_MAX_TEMPLATE = "SELECT MIN(%s), MAX(%s) FROM %s";

  public static String getQuerySql(String database, String table, List<ColumnInfo> columnInfos) {
    String columns = columnInfos.stream()
        .map(ColumnInfo::getName)
        .map(ClickhouseJdbcUtils::getQuoted)
        .collect(Collectors.joining(", "));
    return String.format(SELECT_TEMPLATE, columns, getDbAndTable(database, table));
  }

  public static String getMaxQuerySql(String database, String table, String columnName) {
    return String.format(SELECT_MAX_TEMPLATE, getQuoted(columnName), getDbAndTable(database, table));
  }

  public static String getMinQuerySql(String database, String table, String columnName) {
    return String.format(SELECT_MIN_TEMPLATE, getQuoted(columnName), getDbAndTable(database, table));
  }

  public static String getMinAndMaxQuerySql(String database, String table, String columnName) {
    return String.format(SELECT_MIN_MAX_TEMPLATE,
        getQuoted(columnName), getQuoted(columnName), getDbAndTable(database, table));
  }

  public static String decorateSql(String baseSql,
                                   String splitField,
                                   String filterSql,
                                   Long maxFetchCount,
                                   boolean isPreparedSql) {
    String finalSql = baseSql;

    // where conditions:
    List<String> whereConditions = new ArrayList<>();
    if (isPreparedSql) {
      whereConditions.add(ClickhouseSourceSplit.getRangeClause(splitField));
    }
    whereConditions.add(filterSql);
    String whereSql = whereConditions.stream()
        .filter(StringUtils::isNotEmpty)
        .collect(Collectors.joining(" AND ", " WHERE ", ""));

    // limit conditions
    String limitSql = maxFetchCount == null ? null : " LIMIT " + maxFetchCount + " ";

    // concat
    if (StringUtils.isNotEmpty(whereSql)) {
      finalSql += whereSql;
    }
    if (StringUtils.isNotEmpty(limitSql)) {
      finalSql += limitSql;
    }

    LOG.info("Get query sql: {}", finalSql);
    return finalSql;
  }

  private static String getDbAndTable(String database, String table) {
    database = StringUtils.isEmpty(database) ? null : getQuoted(database);
    table = getQuoted(table);

    return database == null ? table : database + "." + table;
  }

  private static String getQuoted(final String object) {
    return QUOTE_IDENTIFIER + object + QUOTE_IDENTIFIER;
  }
}
