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
import com.bytedance.bitsail.common.ddl.typeinfo.ListTypeInfo;
import com.bytedance.bitsail.common.ddl.typeinfo.MapTypeInfo;
import com.bytedance.bitsail.common.ddl.typeinfo.PrimitiveTypes;
import com.bytedance.bitsail.common.ddl.typeinfo.TypeInfo;
import com.bytedance.bitsail.common.exception.CommonErrorCode;
import com.bytedance.bitsail.common.model.ColumnInfo;
import com.bytedance.bitsail.common.type.BaseEngineTypeInfoConverter;
import com.bytedance.bitsail.common.type.EngineTypeInfoFactory;

import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class NativeFlinkTypeInfoUtil {

  public static RowTypeInfo getRowTypeInformation(String storageEngineName,
                                                  List<ColumnInfo> columnInfos) {

    return getRowTypeInformation(columnInfos, EngineTypeInfoFactory.getEngineConverter(storageEngineName));
  }

  public static RowTypeInfo getRowTypeInformation(List<ColumnInfo> columnInfos,
                                                  BaseEngineTypeInfoConverter baseEngineTypeInfoConverter) {

    String[] fieldNames = new String[columnInfos.size()];
    TypeInformation<?>[] fieldTypes = new TypeInformation[columnInfos.size()];

    for (int index = 0; index < columnInfos.size(); index++) {
      String type = StringUtils.lowerCase(columnInfos.get(index).getType());
      String name = columnInfos.get(index).getName();

      TypeInfo<?> typeInfo = baseEngineTypeInfoConverter.toTypeInfo(type);

      fieldNames[index] = name;
      fieldTypes[index] = toNativeFlinkTypeInformation(typeInfo);
    }

    return new RowTypeInfo(fieldTypes, fieldNames);
  }

  public static TypeInformation<?> getRowTypeInformation(TypeInfo<?>[] typeInfos) {
    List<? extends TypeInformation<?>> tempTypeInfos = Arrays.stream(typeInfos)
        .map(NativeFlinkTypeInfoUtil::toNativeFlinkTypeInformation)
        .collect(Collectors.toList());

    TypeInformation<?>[] types = new TypeInformation<?>[tempTypeInfos.size()];
    for (int index = 0; index < tempTypeInfos.size(); index++) {
      types[index] = tempTypeInfos.get(index);
    }
    return new RowTypeInfo(types);
  }

  public static TypeInformation<?> getTypeInformation(String engineName,
                                                      String engineType) {

    BaseEngineTypeInfoConverter engineConverter = EngineTypeInfoFactory
        .getEngineConverter(engineName);

    TypeInfo<?> typeInfo = engineConverter.toTypeInfo(engineType);

    return toNativeFlinkTypeInformation(typeInfo);
  }

  private static TypeInformation<?> toNativeFlinkTypeInformation(TypeInfo<?> typeInfo) {
    Class<?> internalTypeClass = typeInfo.getTypeClass();
    if (internalTypeClass == PrimitiveTypes.SHORT.getTypeInfo().getTypeClass()) {
      return BasicTypeInfo.SHORT_TYPE_INFO;
    }

    if (internalTypeClass == PrimitiveTypes.VOID.getTypeInfo().getTypeClass()) {
      return BasicTypeInfo.VOID_TYPE_INFO;
    }

    if (internalTypeClass == PrimitiveTypes.INT.getTypeInfo().getTypeClass()) {
      return BasicTypeInfo.INT_TYPE_INFO;
    }

    if (internalTypeClass == PrimitiveTypes.LONG.getTypeInfo().getTypeClass()) {
      return BasicTypeInfo.LONG_TYPE_INFO;
    }

    if (internalTypeClass == PrimitiveTypes.BIGINT.getTypeInfo().getTypeClass()) {
      return BasicTypeInfo.BIG_INT_TYPE_INFO;
    }

    if (internalTypeClass == PrimitiveTypes.FLOAT.getTypeInfo().getTypeClass()) {
      return BasicTypeInfo.FLOAT_TYPE_INFO;
    }

    if (internalTypeClass == PrimitiveTypes.DOUBLE.getTypeInfo().getTypeClass()) {
      return BasicTypeInfo.DOUBLE_TYPE_INFO;
    }

    if (internalTypeClass == PrimitiveTypes.BIG_DECIMAL.getTypeInfo().getTypeClass()) {
      return BasicTypeInfo.BIG_DEC_TYPE_INFO;
    }

    if (internalTypeClass == PrimitiveTypes.STRING.getTypeInfo().getTypeClass()) {
      return BasicTypeInfo.STRING_TYPE_INFO;
    }

    if (internalTypeClass == PrimitiveTypes.BOOLEAN.getTypeInfo().getTypeClass()) {
      return BasicTypeInfo.BOOLEAN_TYPE_INFO;
    }

    if (internalTypeClass == PrimitiveTypes.DATE_DATE.getTypeInfo().getTypeClass()) {
      return SqlTimeTypeInfo.DATE;
    }

    if (internalTypeClass == PrimitiveTypes.DATE_TIME.getTypeInfo().getTypeClass()) {
      return SqlTimeTypeInfo.TIME;
    }

    if (internalTypeClass == PrimitiveTypes.DATE_DATE_TIME.getTypeInfo().getTypeClass()) {
      return SqlTimeTypeInfo.TIMESTAMP;
    }

    if (internalTypeClass == PrimitiveTypes.BYTE.getTypeInfo().getTypeClass()) {
      return BasicTypeInfo.BYTE_TYPE_INFO;
    }

    if (internalTypeClass == PrimitiveTypes.BINARY.getTypeInfo().getTypeClass()) {
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

    throw BitSailException.asBitSailException(CommonErrorCode.CONVERT_NOT_SUPPORT,
        "Custom type info: " + typeInfo + " is not supported in flink!");
  }
}
