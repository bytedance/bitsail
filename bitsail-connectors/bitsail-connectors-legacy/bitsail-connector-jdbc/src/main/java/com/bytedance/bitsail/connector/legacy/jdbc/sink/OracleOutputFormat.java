/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Original Files: apache/flink(https://github.com/apache/flink)
 * Copyright: Copyright 2014-2022 The Apache Software Foundation
 * SPDX-License-Identifier: Apache License 2.0
 */

package com.bytedance.bitsail.connector.legacy.jdbc.sink;

import com.bytedance.bitsail.common.model.ColumnInfo;
import com.bytedance.bitsail.common.util.TypeConvertUtil.StorageEngine;
import com.bytedance.bitsail.connector.legacy.jdbc.constants.WriteModeProxy;
import com.bytedance.bitsail.connector.legacy.jdbc.exception.JDBCPluginErrorCode;
import com.bytedance.bitsail.connector.legacy.jdbc.options.OracleWriterOptions;
import com.bytedance.bitsail.connector.legacy.jdbc.utils.OracleUtil;
import com.bytedance.bitsail.connector.legacy.jdbc.utils.upsert.JDBCUpsertUtil;
import com.bytedance.bitsail.connector.legacy.jdbc.utils.upsert.OracleUpsertUtil;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.commons.lang3.StringUtils.upperCase;

public class OracleOutputFormat extends JDBCOutputFormat {

  String primaryKey;
  String tableSchema;
  String tableWithSchema;

  @Override
  public void initPlugin() throws IOException {
    primaryKey = upperCase(outputSliceConfig.getNecessaryOption(OracleWriterOptions.PRIMARY_KEY, JDBCPluginErrorCode.REQUIRED_VALUE));
    tableSchema = upperCase(outputSliceConfig.getNecessaryOption(OracleWriterOptions.DB_NAME, JDBCPluginErrorCode.REQUIRED_VALUE));
    table = upperCase(outputSliceConfig.getNecessaryOption(OracleWriterOptions.TABLE_NAME, JDBCPluginErrorCode.REQUIRED_VALUE));
    tableWithSchema = tableSchema + "." + table;
    super.initPlugin();
  }

  @Override
  protected List<ColumnInfo> provideColumns() {
    return outputSliceConfig.getNecessaryOption(OracleWriterOptions.COLUMNS, JDBCPluginErrorCode.REQUIRED_VALUE).stream()
        .map(ColumnInfo::toUpperCase)
        .collect(Collectors.toList());
  }

  @Override
  protected String providePartitionName() {
    return upperCase(super.providePartitionName());
  }

  /*
   * Overwrite mode. Get Unique index columns map that could have conflicts.
   */
  @Override
  protected Map<String, List<String>> initUniqueIndexColumnsMap() throws IOException {
    OracleUtil oracleUtil = new OracleUtil();
    try {
      return oracleUtil.getIndexColumnsMap(dbURL, username, password, null, tableSchema, table, true);
    } catch (Exception e) {
      throw new IOException("unable to get unique indexes info, Error: " + e.toString());
    }
  }

  @Override
  protected JDBCUpsertUtil initUpsertUtils() {
    return new OracleUpsertUtil(this, shardKeys, upsertKeys);
  }

  @Override
  public String getDriverName() {
    return OracleUtil.DRIVER_NAME;
  }

  @Override
  public StorageEngine getStorageEngine() {
    return StorageEngine.oracle;
  }

  @Override
  public String getFieldQuote() {
    return OracleUtil.DB_QUOTE;
  }

  @Override
  public String getValueQuote() {
    return OracleUtil.VALUE_QUOTE;
  }

  @Override
  public String getType() {
    return "Oracle";
  }

  /*
   * Generate clear query. Oracle doesn't support 'Delete limit' query. Thus using 'Delete from table where rownum < threshold' instead.
   */
  @Override
  public String genClearQuery(String partitionValue, String compare, String extraPartitionsSql) {
    final String tableWithQuote = getQuoteTable(tableWithSchema);
    final String primaryKeyWithQuote = getQuoteColumn(primaryKey);
    String selectQuery;
    // int or string for "yyyyMMdd" format
    if (partitionPatternFormat.equals("yyyyMMdd")) {
      selectQuery = "select " + primaryKeyWithQuote + " from " + tableWithQuote + " where " + getQuoteColumn(partitionName) +
          compare + wrapPartitionValueWithQuota(partitionValue) + extraPartitionsSql + " and rownum < " + deleteThreshold;
    } else {
      selectQuery = "select " + primaryKeyWithQuote + " from " + tableWithQuote + " where " + getQuoteColumn(partitionName) +
          compare + getQuoteValue(partitionValue) + extraPartitionsSql + " and rownum < " + deleteThreshold;
    }
    return "delete from " + tableWithQuote + " where " + primaryKeyWithQuote + " in (" + selectQuery + ")";
  }

  /*
   * Passing schema information is necessary when creating an oracle insert.
   */
  @Override
  String genInsertQuery(String table, List<ColumnInfo> columns, WriteModeProxy.WriteMode writeMode) {
    return super.genInsertQuery(tableWithSchema, columns, writeMode);
  }

}
