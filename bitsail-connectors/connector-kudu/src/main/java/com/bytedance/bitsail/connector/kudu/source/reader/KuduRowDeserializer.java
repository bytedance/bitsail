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

package com.bytedance.bitsail.connector.kudu.source.reader;

import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.model.ColumnInfo;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.connector.kudu.error.KuduErrorCode;
import com.bytedance.bitsail.connector.kudu.option.KuduReaderOptions;

import org.apache.kudu.client.RowResult;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class KuduRowDeserializer {

  private final List<Function<RowResult, Object>> converters;
  private final int fieldSize;

  public KuduRowDeserializer(BitSailConfiguration jobConf) {
    List<ColumnInfo> columnInfos = jobConf.get(KuduReaderOptions.COLUMNS);
    this.converters = columnInfos.stream().map(this::initWrappedConverter).collect(Collectors.toList());
    this.fieldSize = converters.size();
  }

  public Row convert(RowResult rowResult) {
    Row row = new Row(fieldSize);
    for (int i = 0; i < fieldSize; ++i) {
      row.setField(i, converters.get(i).apply(rowResult));
    }
    return row;
  }

  private Function<RowResult, Object> initWrappedConverter(ColumnInfo columnInfo) {
    String columnName = columnInfo.getName();
    Function<RowResult, Object> converter = initConverter(columnInfo);
    return rowResult -> rowResult.isNull(columnName) ? null : converter.apply(rowResult);
  }

  private Function<RowResult, Object> initConverter(ColumnInfo columnInfo) {
    String columnName = columnInfo.getName();
    String typeName = columnInfo.getType().trim().toUpperCase();

    switch (typeName) {
      case "BOOLEAN": // BOOLEAN_TYPE_INFO
        return rowResult -> rowResult.getBoolean(columnName);
      case "INT8":    // BYTE_TYPE_INFO
        return rowResult -> rowResult.getByte(columnName);
      case "INT16":   // SHORT_TYPE_INFO
        return rowResult -> rowResult.getShort(columnName);
      case "INT32":
      case "INT":     // INT_TYPE_INFO
        return rowResult -> rowResult.getInt(columnName);
      case "INT64":
      case "LONG":    // LONG_TYPE_INFO
        return rowResult -> rowResult.getLong(columnName);
      case "DATE":    // SQL_DATE_TYPE_INFO
        return rowResult -> rowResult.getDate(columnName);
      case "TIMESTAMP":   // SQL_TIMESTAMP_TYPE_INFO
        return rowResult -> rowResult.getTimestamp(columnName);
      case "FLOAT":   // FLOAT_TYPE_INFO
        return rowResult -> rowResult.getFloat(columnName);
      case "DOUBLE":  // DOUBLE_TYPE_INFO
        return rowResult -> rowResult.getDouble(columnName);
      case "DECIMAL": // BIG_DECIMAL_TYPE_INFO
        return rowResult -> rowResult.getDecimal(columnName);
      case "VARCHAR": // STRING_TYPE_INFO
        return rowResult -> rowResult.getVarchar(columnName);
      case "STRING":  // STRING_TYPE_INFO
        return rowResult -> rowResult.getString(columnName);
      case "STRING_UTF8": // BINARY_TYPE_INFO
      case "BINARY":      // BINARY_TYPE_INFO
        return rowResult -> rowResult.getBinary(columnName).array();
      default:
        throw new BitSailException(KuduErrorCode.UNSUPPORTED_TYPE, "Type " + typeName + " is not supported");
    }
  }
}