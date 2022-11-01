/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.bytedance.bitsail.connector.kudu;

import com.google.common.collect.Lists;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.KuduTableStatistics;
import org.apache.kudu.client.RowResult;
import org.apache.kudu.client.RowResultIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class KuduTestUtils {
  private static final Logger LOG = LoggerFactory.getLogger(KuduTestUtils.class);

  public static final int REPLICA_NUM = 1;
  public static final int BUCKET_NUM = 3;

  private static final List<ColumnSchema> COLUMNS;
  private static final Schema SCHEMA;

  static {
    COLUMNS = new ArrayList<>();
    COLUMNS.add(new ColumnSchema.ColumnSchemaBuilder("key", Type.INT64).key(true).build());
    COLUMNS.add(new ColumnSchema.ColumnSchemaBuilder("field_boolean", Type.BOOL).build());
    COLUMNS.add(new ColumnSchema.ColumnSchemaBuilder("field_int", Type.INT32).build());
    COLUMNS.add(new ColumnSchema.ColumnSchemaBuilder("field_double", Type.DOUBLE).build());
    COLUMNS.add(new ColumnSchema.ColumnSchemaBuilder("field_date", Type.DATE).build());
    COLUMNS.add(new ColumnSchema.ColumnSchemaBuilder("field_binary", Type.BINARY).build());
    SCHEMA = new Schema(COLUMNS);
  }


  public static void createTable(KuduClient client, String tableName) {
    CreateTableOptions cto = new CreateTableOptions().setNumReplicas(REPLICA_NUM);
    List<String> hashKeys = Lists.newArrayList("key");
    cto.addHashPartitions(hashKeys, BUCKET_NUM);

    try {
      client.createTable(tableName, SCHEMA, cto);
    } catch (KuduException e) {
      throw new RuntimeException("Failed to create kudu table for test.", e);
    }
    LOG.info("Table {} is created.", tableName);
  }

  public static long getTableRowCount(KuduClient client, String tableName) {
    try {
      KuduTable table = client.openTable(tableName);
      KuduTableStatistics tableStatistics = table.getTableStatistics();
      LOG.info("Table {} statistics: {}", tableName, tableStatistics);
      return tableStatistics.getLiveRowCount();
    } catch (KuduException e) {
      throw new RuntimeException("Failed to get live row count from table: " + tableName, e);
    }
  }

  /**
   * A simple scanner to read the whole table.
   */
  public static List<List<Object>> scanTable(KuduClient client, String tableName) {
    List<String> allColumns = COLUMNS.stream().map(ColumnSchema::getName).collect(Collectors.toList());
    KuduScanner scanner;
    try {
      scanner = client.newScannerBuilder(client.openTable(tableName))
          .setProjectedColumnNames(allColumns)
          .build();
    } catch (KuduException e) {
      throw new RuntimeException("Failed to create scanner for table " + tableName, e);
    }

    List<List<Object>> rows = new ArrayList<>();
    while(scanner.hasMoreRows()) {
      RowResultIterator results;
      try {
        results = scanner.nextRows();
      } catch (KuduException e) {
        throw new RuntimeException("Failed to fetch results from scanner.", e);
      }
      results.forEachRemaining(kuduRow -> rows.add(convert(kuduRow)));
    }

    LOG.info("Found {} rows in table {}", rows.size(), tableName);
    return rows;
  }

  private static List<Object> convert(RowResult kuduRow) {
    List<Object> result = new ArrayList<>();
    result.add(kuduRow.getLong("key"));
    result.add(kuduRow.getBoolean("field_boolean"));
    result.add(kuduRow.getInt("field_int"));
    result.add(kuduRow.getDouble("field_double"));
    result.add(kuduRow.getDate("field_date"));
    result.add(kuduRow.getBinary("field_binary"));
    return result;
  }
}
