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

package com.bytedance.bitsail.test.integration.kudu.container;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.common.typeinfo.TypeInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfos;
import com.bytedance.bitsail.connector.fake.source.FakeRowGenerator;

import com.google.common.collect.Lists;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.ColumnTypeAttributes;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.Insert;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.KuduTableStatistics;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.RowError;
import org.apache.kudu.client.RowErrorsAndOverflowStatus;
import org.apache.kudu.client.RowResult;
import org.apache.kudu.client.RowResultIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class KuduDataSource {
  private static final Logger LOG = LoggerFactory.getLogger(KuduDataSource.class);

  public static final int REPLICA_NUM = 1;
  public static final int BUCKET_NUM = 3;

  private static final List<ColumnSchema> COLUMNS;
  private static final Schema SCHEMA;

  private static final int DECIMAL_TYPE_PRECISION = 30;
  private static final int DECIMAL_TYPE_SCALE = 4;
  private static final int VARCHAR_TYPE_LENGTH = 16;

  static {
    COLUMNS = new ArrayList<>();
    COLUMNS.add(new ColumnSchema.ColumnSchemaBuilder("key", Type.INT64).key(true).build());
    COLUMNS.add(new ColumnSchema.ColumnSchemaBuilder("field_boolean", Type.BOOL).build());
    COLUMNS.add(new ColumnSchema.ColumnSchemaBuilder("field_int8", Type.INT8).build());
    COLUMNS.add(new ColumnSchema.ColumnSchemaBuilder("field_int32", Type.INT32).build());
    COLUMNS.add(new ColumnSchema.ColumnSchemaBuilder("field_float", Type.FLOAT).build());
    COLUMNS.add(new ColumnSchema.ColumnSchemaBuilder("field_double", Type.DOUBLE).build());
    COLUMNS.add(new ColumnSchema.ColumnSchemaBuilder("field_decimal", Type.DECIMAL).typeAttributes(
        new ColumnTypeAttributes.ColumnTypeAttributesBuilder().precision(DECIMAL_TYPE_PRECISION).scale(DECIMAL_TYPE_SCALE).build()).build());
    COLUMNS.add(new ColumnSchema.ColumnSchemaBuilder("field_unix_timestamp", Type.UNIXTIME_MICROS).build());
    COLUMNS.add(new ColumnSchema.ColumnSchemaBuilder("field_date", Type.DATE).build());
    COLUMNS.add(new ColumnSchema.ColumnSchemaBuilder("field_string", Type.STRING).nullable(true).build());
    COLUMNS.add(new ColumnSchema.ColumnSchemaBuilder("field_binary", Type.BINARY).nullable(true).build());
    COLUMNS.add(new ColumnSchema.ColumnSchemaBuilder("field_varchar", Type.VARCHAR).nullable(true).typeAttributes(
        new ColumnTypeAttributes.ColumnTypeAttributesBuilder().length(VARCHAR_TYPE_LENGTH).build()).build());
    SCHEMA = new Schema(COLUMNS);
  }

  public static void createTable(KuduClient client, String tableName) {
    CreateTableOptions cto = new CreateTableOptions().setNumReplicas(REPLICA_NUM);
    List<String> hashKeys = Lists.newArrayList("key");
    cto.addHashPartitions(hashKeys, BUCKET_NUM);

    try {
      if (client.tableExists(tableName)) {
        client.deleteTable(tableName);
      }
      client.createTable(tableName, SCHEMA, cto);
    } catch (KuduException e) {
      throw new RuntimeException("Failed to create kudu table for test.", e);
    }
    LOG.info("Table {} is created.", tableName);
  }

  /**
   * Use {@link com.bytedance.bitsail.connector.fake.source.FakeRowGenerator} to generate random data.
   */
  @SuppressWarnings("checkstyle:MagicNumber")
  public static void insertRandomData(KuduClient client, String tableName, int totalCount) throws KuduException {
    FakeRowGenerator fakeRowGenerator = new FakeRowGenerator(BitSailConfiguration.newDefault(), 1);
    TypeInfo<?>[] typeInfos = {
        TypeInfos.BOOLEAN_TYPE_INFO,
        TypeInfos.BYTE_TYPE_INFO,
        TypeInfos.INT_TYPE_INFO,
        TypeInfos.FLOAT_TYPE_INFO,
        TypeInfos.DOUBLE_TYPE_INFO,
        TypeInfos.BIG_DECIMAL_TYPE_INFO,
        TypeInfos.LOCAL_DATE_TIME_TYPE_INFO,
        TypeInfos.LOCAL_DATE_TYPE_INFO,
        TypeInfos.STRING_TYPE_INFO,
        TypeInfos.STRING_TYPE_INFO,      // getBytes()
        TypeInfos.STRING_TYPE_INFO
    };

    KuduTable kuduTable = client.openTable(tableName);
    KuduSession session = client.newSession();
    for (int i = 0; i < totalCount; ++i) {
      Insert insert = kuduTable.newInsert();
      PartialRow partialRow = insert.getRow();
      Row randomRow = fakeRowGenerator.fakeOneRecord(typeInfos);

      partialRow.addLong("key", i);
      partialRow.addBoolean("field_boolean", randomRow.getBoolean(0));
      partialRow.addByte("field_int8", randomRow.getByte(1));
      partialRow.addInt("field_int32", randomRow.getInt(2));
      partialRow.addFloat("field_float", randomRow.getFloat(3));
      partialRow.addDouble("field_double", randomRow.getDouble(4));
      BigDecimal decimal = randomRow.getDecimal(5, DECIMAL_TYPE_PRECISION, DECIMAL_TYPE_SCALE);
      partialRow.addDecimal("field_decimal", decimal.setScale(DECIMAL_TYPE_SCALE, RoundingMode.DOWN));
      partialRow.addTimestamp("field_unix_timestamp", java.sql.Timestamp.valueOf(randomRow.getLocalDateTime(6)));
      partialRow.addDate("field_date", java.sql.Date.valueOf(randomRow.getLocalDate(7)));
      if (i % 10 == 1) {
        partialRow.setNull("field_string");
      } else {
        partialRow.addString("field_string", randomRow.getString(8));
      }
      if (i % 10 == 2) {
        partialRow.setNull("field_binary");
      } else {
        partialRow.addBinary("field_binary", randomRow.getString(9).getBytes());
      }
      if (i % 10 == 3) {
        partialRow.addVarchar("field_varchar", randomRow.getString(10));
      } else {
        partialRow.setNull("field_varchar");
      }

      session.apply(insert);
    }
    session.close();

    if (session.countPendingErrors() != 0) {
      RowErrorsAndOverflowStatus roStatus = session.getPendingErrors();
      RowError[] errs = roStatus.getRowErrors();

      String errInfo = Arrays.stream(errs)
          .limit(5)
          .map(err -> "[" + err + "]")
          .collect(Collectors.joining(","));
      LOG.error("There were errors when inserting rows to Kudu, the first few errors follow: {}", errInfo);

      if (roStatus.isOverflowed()) {
        LOG.error("Error buffer overflowed. some errors were discarded");
      }
      throw new RuntimeException("Failed to insert rows into Kudu");
    }
    LOG.info("Successfully insert {} rows into table {}", totalCount, tableName);
  }

  /**
   * The live row count can be inaccurate. Please directly scan table and count results.
   */
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
    while (scanner.hasMoreRows()) {
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
    result.add(kuduRow.getInt("field_int32"));
    result.add(kuduRow.getDouble("field_double"));
    result.add(kuduRow.getDate("field_date"));

    if (kuduRow.isNull("field_string")) {
      result.add(null);
    } else {
      result.add(kuduRow.getString("field_string"));
    }

    if (kuduRow.isNull("field_binary")) {
      result.add(null);
    } else {
      result.add(kuduRow.getBinary("field_binary"));
    }
    return result;
  }
}
