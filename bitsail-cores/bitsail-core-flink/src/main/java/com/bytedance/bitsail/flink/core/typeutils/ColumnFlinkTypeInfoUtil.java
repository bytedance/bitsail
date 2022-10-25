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
 */

package com.bytedance.bitsail.flink.core.typeutils;

import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.exception.CommonErrorCode;
import com.bytedance.bitsail.common.model.ColumnInfo;
import com.bytedance.bitsail.common.type.TypeInfoConverter;
import com.bytedance.bitsail.common.typeinfo.BasicArrayTypeInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfos;
import com.bytedance.bitsail.flink.core.typeinfo.ListColumnTypeInfo;
import com.bytedance.bitsail.flink.core.typeinfo.MapColumnTypeInfo;
import com.bytedance.bitsail.flink.core.typeinfo.PrimitiveColumnTypeInfo;

import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * flink type information helper
 *
 * @desc: type system upgrade, after Plugin improvement, FlinkTypeUtil will be deprecated, please use {@link ColumnFlinkTypeInfoUtil}
 */

public class ColumnFlinkTypeInfoUtil {

  public static TypeInfo<?>[] getTypeInfos(TypeInfoConverter converter,
                                           List<ColumnInfo> columnInfos) {

    TypeInfo<?>[] fieldTypes = new TypeInfo[columnInfos.size()];
    for (int index = 0; index < columnInfos.size(); index++) {
      String type = StringUtils.lowerCase(columnInfos.get(index).getType());
      String name = columnInfos.get(index).getName();

      TypeInfo<?> typeInfo = converter.fromTypeString(type);
      fieldTypes[index] = typeInfo;
    }
    return fieldTypes;
  }

  public static RowTypeInfo getRowTypeInformation(TypeInfoConverter converter,
                                                  List<ColumnInfo> columnInfos) {

    String[] fieldNames = new String[columnInfos.size()];
    TypeInformation<?>[] fieldTypes = new TypeInformation[columnInfos.size()];
    for (int index = 0; index < columnInfos.size(); index++) {
      String type = StringUtils.lowerCase(columnInfos.get(index).getType());
      String name = columnInfos.get(index).getName();

      TypeInfo<?> typeInfo = converter.fromTypeString(type);
      fieldNames[index] = name;
      fieldTypes[index] = toColumnFlinkTypeInformation(typeInfo);
    }

    return new RowTypeInfo(fieldTypes, fieldNames);
  }

  private static TypeInformation<?> toColumnFlinkTypeInformation(TypeInfo<?> typeInfo) {
    Class<?> internalTypeClass = typeInfo.getTypeClass();
    if (internalTypeClass == TypeInfos.SHORT_TYPE_INFO.getTypeClass()) {
      return PrimitiveColumnTypeInfo.LONG_COLUMN_TYPE_INFO;
    }

    if (internalTypeClass == TypeInfos.INT_TYPE_INFO.getTypeClass()) {
      return PrimitiveColumnTypeInfo.LONG_COLUMN_TYPE_INFO;
    }

    if (internalTypeClass == TypeInfos.LONG_TYPE_INFO.getTypeClass()) {
      return PrimitiveColumnTypeInfo.LONG_COLUMN_TYPE_INFO;
    }

    if (internalTypeClass == TypeInfos.BIG_INTEGER_TYPE_INFO.getTypeClass()) {
      return PrimitiveColumnTypeInfo.LONG_COLUMN_TYPE_INFO;
    }

    if (internalTypeClass == TypeInfos.FLOAT_TYPE_INFO.getTypeClass()) {
      return PrimitiveColumnTypeInfo.DOUBLE_COLUMN_TYPE_INFO;
    }

    if (internalTypeClass == TypeInfos.DOUBLE_TYPE_INFO.getTypeClass()) {
      return PrimitiveColumnTypeInfo.DOUBLE_COLUMN_TYPE_INFO;
    }

    if (internalTypeClass == TypeInfos.BIG_DECIMAL_TYPE_INFO.getTypeClass()) {
      return PrimitiveColumnTypeInfo.DOUBLE_COLUMN_TYPE_INFO;
    }

    if (internalTypeClass == TypeInfos.STRING_TYPE_INFO.getTypeClass()) {
      return PrimitiveColumnTypeInfo.STRING_COLUMN_TYPE_INFO;
    }

    if (internalTypeClass == TypeInfos.BOOLEAN_TYPE_INFO.getTypeClass()) {
      return PrimitiveColumnTypeInfo.BOOL_COLUMN_TYPE_INFO;
    }

    if (internalTypeClass == TypeInfos.SQL_DATE_TYPE_INFO.getTypeClass()) {
      return PrimitiveColumnTypeInfo.DATE_COLUMN_TYPE_INFO;
    }

    if (internalTypeClass == TypeInfos.SQL_TIME_TYPE_INFO.getTypeClass()) {
      return PrimitiveColumnTypeInfo.DATE_COLUMN_TYPE_INFO;
    }

    if (internalTypeClass == TypeInfos.SQL_TIMESTAMP_TYPE_INFO.getTypeClass()) {
      return PrimitiveColumnTypeInfo.DATE_COLUMN_TYPE_INFO;
    }

    if (internalTypeClass == TypeInfos.BYTE_TYPE_INFO.getTypeClass()) {
      throw new UnsupportedOperationException("Byte is not support in Column type system.");
    }

    if (internalTypeClass == BasicArrayTypeInfo.BINARY_TYPE_INFO.getTypeClass()) {
      return PrimitiveColumnTypeInfo.BYTES_COLUMN_TYPE_INFO;
    }

    if (typeInfo instanceof com.bytedance.bitsail.common.typeinfo.MapTypeInfo) {
      com.bytedance.bitsail.common.typeinfo.MapTypeInfo<?, ?> mapTypeInfo = (com.bytedance.bitsail.common.typeinfo.MapTypeInfo<?, ?>) typeInfo;
      TypeInfo<?> keyTypeInfo = mapTypeInfo.getKeyTypeInfo();
      TypeInfo<?> valueTypeInfo = mapTypeInfo.getValueTypeInfo();
      return new MapColumnTypeInfo(toColumnFlinkTypeInformation(keyTypeInfo),
          toColumnFlinkTypeInformation(valueTypeInfo));
    }

    if (typeInfo instanceof com.bytedance.bitsail.common.typeinfo.ListTypeInfo) {
      com.bytedance.bitsail.common.typeinfo.ListTypeInfo<?> listTypeInfo = (com.bytedance.bitsail.common.typeinfo.ListTypeInfo<?>) typeInfo;
      TypeInfo<?> elementTypeInfo = listTypeInfo.getElementTypeInfo();
      return new ListColumnTypeInfo(toColumnFlinkTypeInformation(elementTypeInfo));
    }

    throw BitSailException.asBitSailException(CommonErrorCode.CONVERT_NOT_SUPPORT,
        "Custom type info: " + typeInfo + " is not supported in custom!");
  }

  public static TypeInformation<?> fromNativeTypeInformation(TypeInformation<?> typeInformation) {
    Class<?> typeClass = typeInformation.getTypeClass();
    if (typeClass == List.class) {

      return fromNativeFlinkListTypeInformation((ListTypeInfo<?>) typeInformation);
    } else if (typeClass == Map.class) {

      return fromNativeFlinkMapTypeInformation((MapTypeInfo<?, ?>) typeInformation);
    } else {

      return fromNativeFlinkPrimitiveTypeInformation(typeInformation);
    }
  }

  private static ListColumnTypeInfo<?> fromNativeFlinkListTypeInformation(ListTypeInfo<?> typeInfo) {
    TypeInformation<?> srcTypeInfo = typeInfo.getElementTypeInfo();
    TypeInformation<?> desTypeInfo = fromNativeTypeInformation(srcTypeInfo);

    return new ListColumnTypeInfo(desTypeInfo);
  }

  private static MapColumnTypeInfo<?, ?> fromNativeFlinkMapTypeInformation(MapTypeInfo<?, ?> typeInfo) {
    TypeInformation<?> srcKeyTypeInfo = typeInfo.getKeyTypeInfo();
    TypeInformation<?> srcValueTypeInfo = typeInfo.getValueTypeInfo();

    TypeInformation<?> desKeyTypeInfo = fromNativeTypeInformation(srcKeyTypeInfo);
    TypeInformation<?> desValueTypeInfo = fromNativeTypeInformation(srcValueTypeInfo);

    return new MapColumnTypeInfo(desKeyTypeInfo, desValueTypeInfo);
  }

  private static TypeInformation<?> fromNativeFlinkPrimitiveTypeInformation(TypeInformation<?> flinkTypeInfo) {
    Class<?> typeClass = flinkTypeInfo.getTypeClass();

    if (typeClass == Integer.class || typeClass == Short.class || typeClass == Long.class || typeClass == BigInteger.class || typeClass == Byte.class) {
      return PrimitiveColumnTypeInfo.LONG_COLUMN_TYPE_INFO;
    } else if (typeClass == Float.class || typeClass == Double.class || typeClass == BigDecimal.class) {
      return PrimitiveColumnTypeInfo.DOUBLE_COLUMN_TYPE_INFO;
    } else if (typeClass == String.class) {
      return PrimitiveColumnTypeInfo.STRING_COLUMN_TYPE_INFO;
    } else if (typeClass == Boolean.class) {
      return PrimitiveColumnTypeInfo.BOOL_COLUMN_TYPE_INFO;
    } else if (typeClass == Date.class || typeClass == java.sql.Date.class || typeClass == java.sql.Time.class || typeClass == java.sql.Timestamp.class) {
      return PrimitiveColumnTypeInfo.DATE_COLUMN_TYPE_INFO;
    } else if (typeClass == Byte[].class || typeClass == byte[].class) {
      return PrimitiveColumnTypeInfo.BYTES_COLUMN_TYPE_INFO;
    } else {
      throw BitSailException.asBitSailException(CommonErrorCode.CONVERT_NOT_SUPPORT,
          "Flink basic data type " + typeClass + " is not supported!");
    }
  }

}
