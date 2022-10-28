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
import com.bytedance.bitsail.common.type.BitSailTypeInfoConverter;
import com.bytedance.bitsail.common.type.TypeInfoConverter;
import com.bytedance.bitsail.common.typeinfo.BasicArrayTypeInfo;
import com.bytedance.bitsail.common.typeinfo.ListTypeInfo;
import com.bytedance.bitsail.common.typeinfo.MapTypeInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfos;

import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.LocalTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

import java.util.List;

public class NativeFlinkTypeInfoUtil {

  public static RowTypeInfo getRowTypeInformation(List<ColumnInfo> columnInfos) {
    return getRowTypeInformation(columnInfos, new BitSailTypeInfoConverter());
  }

  public static RowTypeInfo getRowTypeInformation(List<ColumnInfo> columnInfos,
                                                  TypeInfoConverter typeInfoConverter) {

    String[] fieldNames = new String[columnInfos.size()];
    TypeInformation<?>[] fieldTypes = new TypeInformation[columnInfos.size()];

    for (int index = 0; index < columnInfos.size(); index++) {
      String type = StringUtils.lowerCase(columnInfos.get(index).getType());
      String name = columnInfos.get(index).getName();

      TypeInfo<?> typeInfo = typeInfoConverter.fromTypeString(type);

      fieldNames[index] = name;
      fieldTypes[index] = toNativeFlinkTypeInformation(typeInfo);
    }

    return new RowTypeInfo(fieldTypes, fieldNames);
  }

  public static TypeInformation<Row> getRowTypeInformation(TypeInfo<?>[] typeInfos) {

    TypeInformation<?>[] fieldTypes = new TypeInformation[typeInfos.length];

    for (int index = 0; index < typeInfos.length; index++) {
      fieldTypes[index] = toNativeFlinkTypeInformation(typeInfos[index]);
    }

    return new RowTypeInfo(fieldTypes);
  }

  private static TypeInformation<?> toNativeFlinkTypeInformation(TypeInfo<?> typeInfo) {
    Class<?> internalTypeClass = typeInfo.getTypeClass();
    if (internalTypeClass == TypeInfos.SHORT_TYPE_INFO.getTypeClass()) {
      return BasicTypeInfo.SHORT_TYPE_INFO;
    }

    if (internalTypeClass == TypeInfos.VOID_TYPE_INFO.getTypeClass()) {
      return BasicTypeInfo.VOID_TYPE_INFO;
    }

    if (internalTypeClass == TypeInfos.INT_TYPE_INFO.getTypeClass()) {
      return BasicTypeInfo.INT_TYPE_INFO;
    }

    if (internalTypeClass == TypeInfos.LONG_TYPE_INFO.getTypeClass()) {
      return BasicTypeInfo.LONG_TYPE_INFO;
    }

    if (internalTypeClass == TypeInfos.BIG_INTEGER_TYPE_INFO.getTypeClass()) {
      return BasicTypeInfo.BIG_INT_TYPE_INFO;
    }

    if (internalTypeClass == TypeInfos.FLOAT_TYPE_INFO.getTypeClass()) {
      return BasicTypeInfo.FLOAT_TYPE_INFO;
    }

    if (internalTypeClass == TypeInfos.DOUBLE_TYPE_INFO.getTypeClass()) {
      return BasicTypeInfo.DOUBLE_TYPE_INFO;
    }

    if (internalTypeClass == TypeInfos.BIG_DECIMAL_TYPE_INFO.getTypeClass()) {
      return BasicTypeInfo.BIG_DEC_TYPE_INFO;
    }

    if (internalTypeClass == TypeInfos.STRING_TYPE_INFO.getTypeClass()) {
      return BasicTypeInfo.STRING_TYPE_INFO;
    }

    if (internalTypeClass == TypeInfos.BOOLEAN_TYPE_INFO.getTypeClass()) {
      return BasicTypeInfo.BOOLEAN_TYPE_INFO;
    }

    if (internalTypeClass == TypeInfos.SQL_DATE_TYPE_INFO.getTypeClass()) {
      return SqlTimeTypeInfo.DATE;
    }

    if (internalTypeClass == TypeInfos.SQL_TIME_TYPE_INFO.getTypeClass()) {
      return SqlTimeTypeInfo.TIME;
    }

    if (internalTypeClass == TypeInfos.SQL_TIMESTAMP_TYPE_INFO.getTypeClass()) {
      return SqlTimeTypeInfo.TIMESTAMP;
    }

    if (internalTypeClass == TypeInfos.LOCAL_DATE_TYPE_INFO.getTypeClass()) {
      return LocalTimeTypeInfo.LOCAL_DATE;
    }

    if (internalTypeClass == TypeInfos.LOCAL_TIME_TYPE_INFO.getTypeClass()) {
      return LocalTimeTypeInfo.LOCAL_TIME;
    }

    if (internalTypeClass == TypeInfos.LOCAL_DATE_TIME_TYPE_INFO.getTypeClass()) {
      return LocalTimeTypeInfo.LOCAL_DATE_TIME;
    }

    if (internalTypeClass == TypeInfos.BYTE_TYPE_INFO.getTypeClass()) {
      return BasicTypeInfo.BYTE_TYPE_INFO;
    }

    if (internalTypeClass == BasicArrayTypeInfo.BINARY_TYPE_INFO.getTypeClass()) {
      return PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO;
    }

    if (typeInfo instanceof MapTypeInfo) {
      MapTypeInfo<?, ?> mapTypeInfo = (MapTypeInfo<?, ?>) typeInfo;
      TypeInfo<?> keyTypeInfo = mapTypeInfo.getKeyTypeInfo();
      TypeInfo<?> valueTypeInfo = mapTypeInfo.getValueTypeInfo();
      return new org.apache.flink.api.java.typeutils.MapTypeInfo<>(toNativeFlinkTypeInformation(keyTypeInfo),
          toNativeFlinkTypeInformation(valueTypeInfo));
    }

    if (typeInfo instanceof ListTypeInfo) {
      ListTypeInfo<?> listTypeInfo = (ListTypeInfo<?>) typeInfo;
      TypeInfo<?> elementTypeInfo = listTypeInfo.getElementTypeInfo();
      return new org.apache.flink.api.java.typeutils.ListTypeInfo<>(toNativeFlinkTypeInformation(elementTypeInfo));
    }

    throw BitSailException.asBitSailException(CommonErrorCode.CONVERT_NOT_SUPPORT, String
        .format("BitSail type info %s not support in flink runtime.", typeInfo));
  }
}
