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
import java.math.RoundingMode;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

public class KuduRowBuilder {

  private final List<BiConsumer<PartialRow, Object>> rowInserters;
  private final int fieldSize;

  private final Schema schema;

  public KuduRowBuilder(BitSailConfiguration jobConf, Schema schema) {
    List<ColumnInfo> columnInfos = jobConf.get(KuduWriterOptions.COLUMNS);
    this.schema = schema;
    if (columnInfos != null) {
      this.fieldSize = columnInfos.size();
      this.rowInserters = new ArrayList<>(fieldSize);
      columnInfos.forEach(columnInfo -> {
        ColumnSchema columnSchema = schema.getColumn(columnInfo.getName());
        rowInserters.add(columnSchema.isNullable() ?
            initNullableRowInserter(columnInfo) : initRowInserter(columnInfo));
      });
    } else {
      this.fieldSize = this.schema.getColumnCount();
      this.rowInserters = new ArrayList<>(fieldSize);
      this.schema.getColumns().forEach(columnSchema -> {
        ColumnInfo columnInfo = new ColumnInfo(columnSchema.getName(), columnSchema.getType().getName());
        rowInserters.add(columnSchema.isNullable() ?
            initNullableRowInserter(columnInfo) : initRowInserter(columnInfo));
      });
    }
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
      case "BOOL":
      case "BOOLEAN": // BOOLEAN_TYPE_INFO
        return (PartialRow kuduRow, Object value) -> kuduRow.addBoolean(columnName, (boolean) value);
      case "INT8":    // BYTE_TYPE_INFO
        return (PartialRow kuduRow, Object value) -> {
          if (value instanceof Long) {
            kuduRow.addByte(columnName, ((Long) value).byteValue());
          } else {
            kuduRow.addByte(columnName, (byte) value);
          }
        };
      case "INT16":   // SHORT_TYPE_INFO
        return (PartialRow kuduRow, Object value) -> {
          if (value instanceof Long) {
            kuduRow.addShort(columnName, ((Long) value).shortValue());
          } else {
            kuduRow.addShort(columnName, (short) value);
          }
        };
      case "INT32":
      case "INT":     // INT_TYPE_INFO
        return (PartialRow kuduRow, Object value) -> {
          if (value instanceof Long) {
            kuduRow.addInt(columnName, ((Long) value).intValue());
          } else {
            kuduRow.addInt(columnName, (int) value);
          }
        };
      case "INT64":
      case "LONG":    // LONG_TYPE_INFO
        return (PartialRow kuduRow, Object value) -> kuduRow.addLong(columnName, (long) value);
      case "DATE":    // SQL_DATE_TYPE_INFO
        return (PartialRow kuduRow, Object value) -> {
          if (value instanceof Timestamp) {
            kuduRow.addDate(columnName, new Date(((Timestamp) value).getTime()));
          } else if (value instanceof LocalDate) {
            kuduRow.addDate(columnName, Date.valueOf((LocalDate) value));
          } else if (value instanceof LocalDateTime) {
            kuduRow.addDate(columnName, Date.valueOf(((LocalDateTime) value).toLocalDate()));
          } else if (value instanceof java.util.Date) {
            kuduRow.addDate(columnName, new Date(((java.util.Date) value).getTime()));
          } else {
            kuduRow.addDate(columnName, (Date) value);
          }
        };
      case "UNIXTIME_MICROS":
      case "TIMESTAMP":   // SQL_TIMESTAMP_TYPE_INFO
        return (PartialRow kuduRow, Object value) -> {
          if (value instanceof Timestamp) {
            kuduRow.addTimestamp(columnName, (Timestamp) value);
          } else if (value instanceof LocalDateTime) {
            kuduRow.addTimestamp(columnName, Timestamp.valueOf((LocalDateTime) value));
          } else if (value instanceof LocalDate) {
            kuduRow.addTimestamp(columnName, Timestamp.valueOf(((LocalDate) value).atStartOfDay()));
          } else if (value instanceof java.util.Date) {
            kuduRow.addTimestamp(columnName, new Timestamp(((java.util.Date) value).getTime()));
          } else if (value instanceof Long) {
            kuduRow.addLong(columnName, (Long) value);
          } else {
            throw new BitSailException(KuduErrorCode.ILLEGAL_VALUE, "Value " + value + " is not Long or Timestamp.");
          }
        };
      case "FLOAT":   // FLOAT_TYPE_INFO
        return (PartialRow kuduRow, Object value) -> {
          if (value instanceof Double) {
            kuduRow.addFloat(columnName, ((Double) value).floatValue());
          } else {
            kuduRow.addFloat(columnName, (float) value);
          }
        };
      case "DOUBLE":  // DOUBLE_TYPE_INFO
        return (PartialRow kuduRow, Object value) -> kuduRow.addDouble(columnName, (double) value);
      case "DECIMAL": // BIG_DECIMAL_TYPE_INFO
        // scala align
        final ColumnSchema columnSchema = schema.getColumn(columnInfo.getName());
        final int scala = columnSchema.getTypeAttributes().getScale();
        return (PartialRow kuduRow, Object value) -> {
          if (value instanceof Double) {
            kuduRow.addDecimal(columnName, BigDecimal.valueOf((double) value).setScale(scala, RoundingMode.DOWN));
          } else {
            kuduRow.addDecimal(columnName, ((BigDecimal) value).setScale(scala, RoundingMode.DOWN));
          }
        };
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
