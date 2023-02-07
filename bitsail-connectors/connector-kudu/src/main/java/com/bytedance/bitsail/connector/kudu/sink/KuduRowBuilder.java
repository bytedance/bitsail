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

package com.bytedance.bitsail.connector.kudu.sink;

import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.model.ColumnInfo;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.connector.kudu.error.KuduErrorCode;
import com.bytedance.bitsail.connector.kudu.option.KuduWriterOptions;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.PartialRow;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

public class KuduRowBuilder {

  private final List<BiConsumer<PartialRow, Object>> rowInserters;
  private final int fieldSize;

  public KuduRowBuilder(BitSailConfiguration jobConf, Schema schema) {
    List<ColumnInfo> columnInfos = jobConf.get(KuduWriterOptions.COLUMNS);
    this.fieldSize = columnInfos.size();

    this.rowInserters = new ArrayList<>(fieldSize);
    columnInfos.forEach(columnInfo -> {
      ColumnSchema columnSchema = schema.getColumn(columnInfo.getName());
      rowInserters.add(columnSchema.isNullable() ?
          initNullableRowInserter(columnInfo) : initRowInserter(columnInfo));
    });
  }

  /**
   * Transform a bitsail row to KuduRow.
   */
  public void build(PartialRow kuduRow, Row row) {
    for (int i = 0; i < fieldSize; ++i) {
      rowInserters.get(i).accept(kuduRow, row.getField(i));
    }
  }

  private BiConsumer<PartialRow, Object> initNullableRowInserter(ColumnInfo columnInfo) {
    String columnName = columnInfo.getName();
    BiConsumer<PartialRow, Object> rowInserter = initRowInserter(columnInfo);

    return (PartialRow kuduRow, Object value) -> {
      if (value == null) {
        kuduRow.setNull(columnName);
      } else {
        rowInserter.accept(kuduRow, value);
      }
    };
  }

  private BiConsumer<PartialRow, Object> initRowInserter(ColumnInfo columnInfo) {
    String columnName = columnInfo.getName();
    String typeName = columnInfo.getType().trim().toUpperCase();

    switch (typeName) {
      case "BOOLEAN": // BOOLEAN_TYPE_INFO
        return (PartialRow kuduRow, Object value) -> kuduRow.addBoolean(columnName, (boolean) value);
      case "INT8":    // BYTE_TYPE_INFO
        return (PartialRow kuduRow, Object value) -> kuduRow.addByte(columnName, (byte) value);
      case "INT16":   // SHORT_TYPE_INFO
        return (PartialRow kuduRow, Object value) -> kuduRow.addShort(columnName, (short) value);
      case "INT32":
      case "INT":     // INT_TYPE_INFO
        return (PartialRow kuduRow, Object value) -> kuduRow.addInt(columnName, (int) value);
      case "INT64":
      case "LONG":    // LONG_TYPE_INFO
        return (PartialRow kuduRow, Object value) -> kuduRow.addLong(columnName, (long) value);
      case "DATE":    // SQL_DATE_TYPE_INFO
        return (PartialRow kuduRow, Object value) -> kuduRow.addDate(columnName, (Date) value);
      case "TIMESTAMP":   // SQL_TIMESTAMP_TYPE_INFO
        return (PartialRow kuduRow, Object value) -> {
          if (value instanceof Timestamp) {
            kuduRow.addTimestamp(columnName, (Timestamp) value);
          } else if (value instanceof Long) {
            kuduRow.addLong(columnName, (Long) value);
          } else {
            throw new BitSailException(KuduErrorCode.ILLEGAL_VALUE, "Value " + value + " is not Long or Timestamp.");
          }
        };
      case "FLOAT":   // FLOAT_TYPE_INFO
        return (PartialRow kuduRow, Object value) -> kuduRow.addFloat(columnName, (float) value);
      case "DOUBLE":  // DOUBLE_TYPE_INFO
        return (PartialRow kuduRow, Object value) -> kuduRow.addDouble(columnName, (double) value);
      case "DECIMAL": // BIG_DECIMAL_TYPE_INFO
        return (PartialRow kuduRow, Object value) -> kuduRow.addDecimal(columnName, (BigDecimal) value);
      case "VARCHAR": // STRING_TYPE_INFO
        return (PartialRow kuduRow, Object value) -> kuduRow.addVarchar(columnName, value.toString());
      case "STRING":  // STRING_TYPE_INFO
        return (PartialRow kuduRow, Object value) -> kuduRow.addString(columnName, value.toString());
      case "STRING_UTF8": // BINARY_TYPE_INFO
        return (PartialRow kuduRow, Object value) -> kuduRow.addStringUtf8(columnName, (byte[]) value);
      case "BINARY":  // BINARY_TYPE_INFO
        return (PartialRow kuduRow, Object value) -> kuduRow.addBinary(columnName, (byte[]) value);
      default:
        throw new BitSailException(KuduErrorCode.UNSUPPORTED_TYPE, "Type " + typeName + " is not supported");
    }
  }
}
