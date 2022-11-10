/*
 *     Licensed to the Apache Software Foundation (ASF) under one or more
 *     contributor license agreements.  See the NOTICE file distributed with
 *     this work for additional information regarding copyright ownership.
 *     The ASF licenses this file to You under the Apache License, Version 2.0
 *     (the "License"); you may not use this file except in compliance with
 *     the License.  You may obtain a copy of the License at
 *        http://www.apache.org/licenses/LICENSE-2.0
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 */

package com.bytedance.bitsail.connector.clickhouse.source.reader;

import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.model.ColumnInfo;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.connector.clickhouse.error.ClickhouseErrorCode;
import com.bytedance.bitsail.connector.clickhouse.option.ClickhouseReaderOptions;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ClickhouseRowDeserializer {

  interface FiledConverter {
    Object apply(ResultSet resultSet) throws SQLException;
  }

  private final List<FiledConverter> converters;
  private final int fieldSize;

  public ClickhouseRowDeserializer(BitSailConfiguration jobConf) {
    List<ColumnInfo> columnInfos = jobConf.get(ClickhouseReaderOptions.COLUMNS);

    this.fieldSize = columnInfos.size();
    this.converters = new ArrayList<>();
    for (int i = 0; i < fieldSize; ++i) {
      converters.add(initFieldConverter(i + 1, columnInfos.get(i)));
    }
  }

  public Row convert(ResultSet resultSet) {
    Row row = new Row(fieldSize);
    try {
      for (int i = 0; i < fieldSize; ++i) {
        row.setField(i, converters.get(i).apply(resultSet));
      }
    } catch (SQLException e) {
      throw BitSailException.asBitSailException(ClickhouseErrorCode.CONVERT_ERROR, e.getCause());
    }
    return row;
  }

  private FiledConverter initFieldConverter(int index, ColumnInfo columnInfo) {
    String typeName = columnInfo.getType().trim().toUpperCase();
    if (typeName.contains("DECIMAL")) {
      typeName = "DECIMAL";
    }

    switch (typeName) {
      case "INT8":
        return resultSet -> resultSet.getByte(index);
      case "UINT8":
      case "INT16":
        return resultSet -> resultSet.getShort(index);
      case "UINT16":
      case "INT":
      case "INT32":
        return resultSet -> resultSet.getInt(index);
      case "UINT32":
      case "LONG":
      case "INT64":
        return resultSet -> resultSet.getLong(index);
      case "UINT64":
      case "BIGINT":
        return resultSet -> {
          BigDecimal dec = resultSet.getBigDecimal(index);
          return dec == null ? null : dec.toBigInteger();
        };

      case "FLOAT":
      case "FLOAT32":
        return resultSet -> resultSet.getFloat(index);
      case "DOUBLE":
      case "FLOAT64":
        return resultSet -> resultSet.getDouble(index);
      case "DECIMAL":
        return resultSet -> resultSet.getBigDecimal(index);

      case "CHAR":
      case "VARCHAR":
      case "TEXT":
      case "STRING":
        return resultSet -> resultSet.getString(index);

      case "DATE":
        return resultSet -> resultSet.getDate(index);
      case "DATETIME":
      case "TIMESTAMP":
        return resultSet -> resultSet.getTimestamp(index);
      case "TIME":
        return resultSet -> resultSet.getTime(index);

      case "BOOLEAN":
        return resultSet -> resultSet.getBoolean(index);

      case "NULL":
        return resultSet -> null;

      default:
        throw new UnsupportedOperationException("Unsupported data type: " + typeName);
    }
  }

}
