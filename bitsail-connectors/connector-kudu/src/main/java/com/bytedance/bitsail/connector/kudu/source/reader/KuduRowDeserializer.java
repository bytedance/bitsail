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
import com.bytedance.bitsail.common.typeinfo.BasicArrayTypeInfo;
import com.bytedance.bitsail.common.typeinfo.RowTypeInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfos;

import org.apache.kudu.client.RowResult;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class KuduRowDeserializer {

  private final List<Function<RowResult, Object>> converters;
  private final int fieldSize;

  public KuduRowDeserializer(RowTypeInfo rowTypeInfo) {
    this.converters = new ArrayList <>();
    this.fieldSize = rowTypeInfo.getTypeInfos().length;
    for (int i = 0; i < fieldSize; ++i) {
      converters.add(initWrappedConverter(rowTypeInfo.getFieldNames()[i], rowTypeInfo.getTypeInfos()[i]));
    }
  }

  public Row convert(RowResult rowResult) {
    Row row = new Row(fieldSize);
    for (int i = 0; i < fieldSize; ++i) {
      row.setField(i, converters.get(i).apply(rowResult));
    }
    return row;
  }

  private Function<RowResult, Object> initWrappedConverter(String columnName, TypeInfo<?> typeInfo) {
    Function<RowResult, Object> converter = initConverter(columnName, typeInfo);
    return rowResult -> rowResult.isNull(columnName) ? null : converter.apply(rowResult);
  }

  private Function<RowResult, Object> initConverter(String columnName, TypeInfo<?> typeInfo) {
    Class<?> curClass = typeInfo.getTypeClass();
    if (TypeInfos.BOOLEAN_TYPE_INFO.getTypeClass() == curClass) {
      return rowResult -> rowResult.getBoolean(columnName);
    }
    if (TypeInfos.BYTE_TYPE_INFO.getTypeClass() == curClass) {
      return rowResult -> rowResult.getByte(columnName);
    }
    if (TypeInfos.SHORT_TYPE_INFO.getTypeClass() == curClass) {
      return rowResult -> rowResult.getShort(columnName);
    }
    if (TypeInfos.INT_TYPE_INFO.getTypeClass() == curClass) {
      return rowResult -> rowResult.getInt(columnName);
    }
    if (TypeInfos.LONG_TYPE_INFO.getTypeClass() == curClass) {
      return rowResult -> rowResult.getLong(columnName);
    }
    if (TypeInfos.LOCAL_DATE_TYPE_INFO.getTypeClass() == curClass) {
      return rowResult -> rowResult.getDate(columnName).toLocalDate();
    }
    if (TypeInfos.LOCAL_DATE_TIME_TYPE_INFO.getTypeClass() == curClass) {
      return rowResult -> rowResult.getTimestamp(columnName).toLocalDateTime();
    }
    if (TypeInfos.FLOAT_TYPE_INFO.getTypeClass() == curClass) {
      return rowResult -> rowResult.getFloat(columnName);
    }
    if (TypeInfos.DOUBLE_TYPE_INFO.getTypeClass() == curClass) {
      return rowResult -> rowResult.getDouble(columnName);
    }
    if (TypeInfos.BIG_DECIMAL_TYPE_INFO.getTypeClass() == curClass) {
      return rowResult -> rowResult.getDecimal(columnName);
    }
    if (TypeInfos.STRING_TYPE_INFO.getTypeClass() == curClass) {
      return rowResult -> rowResult.getString(columnName);
    }
    if (BasicArrayTypeInfo.BINARY_TYPE_INFO.getTypeClass() == curClass) {
      return rowResult -> rowResult.getBinary(columnName).array();
    }
    throw BitSailException.asBitSailException(CommonErrorCode.UNSUPPORTED_COLUMN_TYPE, typeInfo.getTypeClass().getName() + " is not supported yet.");
  }
}