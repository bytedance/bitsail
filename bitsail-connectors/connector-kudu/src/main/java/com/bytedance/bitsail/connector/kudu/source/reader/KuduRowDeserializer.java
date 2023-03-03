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
import com.bytedance.bitsail.common.exception.CommonErrorCode;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.common.typeinfo.BasicTypeInfo;
import com.bytedance.bitsail.common.typeinfo.RowTypeInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfos;
import com.bytedance.bitsail.connector.kudu.error.KuduErrorCode;

import org.apache.kudu.client.RowResult;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class KuduRowDeserializer {

  interface FiledConverter {
    Object apply(RowResult rowResult) throws SQLException;
  }

  private final List<FiledConverter> converters;
  private final int fieldSize;

  public KuduRowDeserializer(RowTypeInfo rowTypeInfo) {
    this.converters = new ArrayList <>();
    this.fieldSize = rowTypeInfo.getTypeInfos().length;
    for (int i = 0; i < fieldSize; ++i) {
      converters.add(initFieldConverter(i + 1, rowTypeInfo.getTypeInfos()[i]));
    }
  }

  public Row convert(RowResult rowResult) {
    Row row = new Row(fieldSize);
    try {
      for (int i = 0; i < fieldSize; ++i) {
        row.setField(i, converters.get(i).apply(rowResult));
      }
    } catch (SQLException e) {
      throw BitSailException.asBitSailException(KuduErrorCode.UNSUPPORTED_TYPE, e.getCause());
    }
    return row;
  }

  private FiledConverter initFieldConverter(int index, TypeInfo <?> typeInfo) {
    if (!(typeInfo instanceof BasicTypeInfo)) {
      throw BitSailException.asBitSailException(CommonErrorCode.UNSUPPORTED_COLUMN_TYPE, typeInfo.getTypeClass().getName() + " is not supported yet.");
    }

    Class<?> curClass = typeInfo.getTypeClass();
    if (TypeInfos.BOOLEAN_TYPE_INFO.getTypeClass() == curClass) {
      return resultSet -> resultSet.getBoolean(index);
    }
    if (TypeInfos.BYTE_TYPE_INFO.getTypeClass() == curClass) {
      return resultSet -> resultSet.getByte(index);
    }
    if (TypeInfos.SHORT_TYPE_INFO.getTypeClass() == curClass) {
      return resultSet -> resultSet.getShort(index);
    }
    if (TypeInfos.INT_TYPE_INFO.getTypeClass() == curClass) {
      return resultSet -> resultSet.getInt(index);
    }
    if (TypeInfos.LONG_TYPE_INFO.getTypeClass() == curClass) {
      return resultSet -> resultSet.getLong(index);
    }
    if (TypeInfos.SQL_DATE_TYPE_INFO.getTypeClass() == curClass) {
      return resultSet -> resultSet.getDate(index);
    }
    if (TypeInfos.SQL_TIMESTAMP_TYPE_INFO.getTypeClass() == curClass) {
      return resultSet -> resultSet.getTimestamp(index);
    }
    if (TypeInfos.FLOAT_TYPE_INFO.getTypeClass() == curClass) {
      return resultSet -> resultSet.getFloat(index);
    }
    if (TypeInfos.DOUBLE_TYPE_INFO.getTypeClass() == curClass) {
      return resultSet -> resultSet.getDouble(index);
    }
    if (TypeInfos.BIG_DECIMAL_TYPE_INFO.getTypeClass() == curClass) {
      return resultSet -> resultSet.getDecimal(index);
    }
    if (TypeInfos.STRING_TYPE_INFO.getTypeClass() == curClass) {
      return resultSet -> resultSet.getString(index);
    }
    if (TypeInfos.BINARY_TYPE_INFO.getTypeClass() == curClass) {
      return resultSet -> resultSet.getBinary(index);
    }
    throw new UnsupportedOperationException("Unsupported data type: " + typeInfo);
  }
}