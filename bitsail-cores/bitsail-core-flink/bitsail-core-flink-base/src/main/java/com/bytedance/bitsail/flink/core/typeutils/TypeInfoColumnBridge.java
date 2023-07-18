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

package com.bytedance.bitsail.flink.core.typeutils;

import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.exception.CommonErrorCode;
import com.bytedance.bitsail.common.typeinfo.BasicArrayTypeInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfos;
import com.bytedance.bitsail.flink.core.typeinfo.PrimitiveColumnTypeInfo;

import com.google.common.collect.Maps;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.util.Map;
import java.util.Objects;

/**
 * Bridge class for {@link TypeInfo} and {@link PrimitiveColumnTypeInfo}
 */
public class TypeInfoColumnBridge {

  public static final Map<TypeInfo<?>, TypeInformation<?>> COLUMN_BRIDGE_TYPE_INFO_MAPPING =
      Maps.newHashMap();

  public static final Map<Class<?>, TypeInformation<?>> COLUMN_BRIDGE_CLASS_MAPPING =
      Maps.newHashMap();

  public static final Map<Class<?>, TypeInfo<?>> TYPE_INFO_BRIDGE_CLASS_MAPPING =
      Maps.newHashMap();

  static {
    COLUMN_BRIDGE_TYPE_INFO_MAPPING.put(TypeInfos.BYTE_TYPE_INFO,
        PrimitiveColumnTypeInfo.LONG_COLUMN_TYPE_INFO);
    COLUMN_BRIDGE_TYPE_INFO_MAPPING.put(TypeInfos.SHORT_TYPE_INFO,
        PrimitiveColumnTypeInfo.LONG_COLUMN_TYPE_INFO);
    COLUMN_BRIDGE_TYPE_INFO_MAPPING.put(TypeInfos.INT_TYPE_INFO,
        PrimitiveColumnTypeInfo.LONG_COLUMN_TYPE_INFO);
    COLUMN_BRIDGE_TYPE_INFO_MAPPING.put(TypeInfos.LONG_TYPE_INFO,
        PrimitiveColumnTypeInfo.LONG_COLUMN_TYPE_INFO);
    COLUMN_BRIDGE_TYPE_INFO_MAPPING.put(TypeInfos.BIG_INTEGER_TYPE_INFO,
        PrimitiveColumnTypeInfo.LONG_COLUMN_TYPE_INFO);

    COLUMN_BRIDGE_TYPE_INFO_MAPPING.put(TypeInfos.FLOAT_TYPE_INFO,
        PrimitiveColumnTypeInfo.DOUBLE_COLUMN_TYPE_INFO);
    COLUMN_BRIDGE_TYPE_INFO_MAPPING.put(TypeInfos.DOUBLE_TYPE_INFO,
        PrimitiveColumnTypeInfo.DOUBLE_COLUMN_TYPE_INFO);
    COLUMN_BRIDGE_TYPE_INFO_MAPPING.put(TypeInfos.BIG_DECIMAL_TYPE_INFO,
        PrimitiveColumnTypeInfo.DOUBLE_COLUMN_TYPE_INFO);

    COLUMN_BRIDGE_TYPE_INFO_MAPPING.put(TypeInfos.STRING_TYPE_INFO,
        PrimitiveColumnTypeInfo.STRING_COLUMN_TYPE_INFO);
    COLUMN_BRIDGE_TYPE_INFO_MAPPING.put(TypeInfos.BOOLEAN_TYPE_INFO,
        PrimitiveColumnTypeInfo.BOOL_COLUMN_TYPE_INFO);

    COLUMN_BRIDGE_TYPE_INFO_MAPPING.put(TypeInfos.SQL_DATE_TYPE_INFO,
        PrimitiveColumnTypeInfo.DATE_COLUMN_TYPE_INFO);
    COLUMN_BRIDGE_TYPE_INFO_MAPPING.put(TypeInfos.SQL_TIME_TYPE_INFO,
        PrimitiveColumnTypeInfo.DATE_COLUMN_TYPE_INFO);
    COLUMN_BRIDGE_TYPE_INFO_MAPPING.put(TypeInfos.SQL_TIMESTAMP_TYPE_INFO,
        PrimitiveColumnTypeInfo.DATE_COLUMN_TYPE_INFO);
    COLUMN_BRIDGE_TYPE_INFO_MAPPING.put(TypeInfos.LOCAL_DATE_TYPE_INFO,
        PrimitiveColumnTypeInfo.DATE_COLUMN_TYPE_INFO);
    COLUMN_BRIDGE_TYPE_INFO_MAPPING.put(TypeInfos.LOCAL_TIME_TYPE_INFO,
        PrimitiveColumnTypeInfo.DATE_COLUMN_TYPE_INFO);
    COLUMN_BRIDGE_TYPE_INFO_MAPPING.put(TypeInfos.LOCAL_DATE_TIME_TYPE_INFO,
        PrimitiveColumnTypeInfo.DATE_COLUMN_TYPE_INFO);

    COLUMN_BRIDGE_TYPE_INFO_MAPPING.put(BasicArrayTypeInfo.BINARY_TYPE_INFO,
        PrimitiveColumnTypeInfo.BYTES_COLUMN_TYPE_INFO);

    for (TypeInfo<?> typeInfo : COLUMN_BRIDGE_TYPE_INFO_MAPPING.keySet()) {
      COLUMN_BRIDGE_CLASS_MAPPING.put(typeInfo.getTypeClass(),
          COLUMN_BRIDGE_TYPE_INFO_MAPPING.get(typeInfo));
    }

    //According to column type info not match with framework type info, so we decide use fixed mapping for it.
    //TODO after we deprecated column type info we will delete those logic.
    TYPE_INFO_BRIDGE_CLASS_MAPPING.put(
        PrimitiveColumnTypeInfo.LONG_COLUMN_TYPE_INFO.getTypeClass(),
        TypeInfos.LONG_TYPE_INFO);

    TYPE_INFO_BRIDGE_CLASS_MAPPING.put(
        PrimitiveColumnTypeInfo.DOUBLE_COLUMN_TYPE_INFO.getTypeClass(),
        TypeInfos.DOUBLE_TYPE_INFO);

    TYPE_INFO_BRIDGE_CLASS_MAPPING.put(
        PrimitiveColumnTypeInfo.STRING_COLUMN_TYPE_INFO.getTypeClass(),
        TypeInfos.STRING_TYPE_INFO);

    TYPE_INFO_BRIDGE_CLASS_MAPPING.put(
        PrimitiveColumnTypeInfo.BOOL_COLUMN_TYPE_INFO.getTypeClass(),
        TypeInfos.BOOLEAN_TYPE_INFO);

    TYPE_INFO_BRIDGE_CLASS_MAPPING.put(
        PrimitiveColumnTypeInfo.DATE_COLUMN_TYPE_INFO.getTypeClass(),
        TypeInfos.SQL_TIMESTAMP_TYPE_INFO);

    TYPE_INFO_BRIDGE_CLASS_MAPPING.put(
        PrimitiveColumnTypeInfo.BYTES_COLUMN_TYPE_INFO.getTypeClass(),
        BasicArrayTypeInfo.BINARY_TYPE_INFO);
  }

  public static TypeInformation<?> bridgeTypeInfo(TypeInfo<?> bridge) {
    TypeInformation<?> typeInformation = COLUMN_BRIDGE_CLASS_MAPPING.get(bridge.getTypeClass());
    if (Objects.isNull(typeInformation)) {
      throw BitSailException.asBitSailException(CommonErrorCode.CONVERT_NOT_SUPPORT, String
          .format("Primitive type info %s has no column bridge type information.", bridge));
    }
    return typeInformation;
  }

  public static TypeInfo<?> bridgeTypeInformation(TypeInformation<?> bridge) {
    TypeInfo<?> typeInfo = TYPE_INFO_BRIDGE_CLASS_MAPPING.get(bridge.getTypeClass());
    if (Objects.isNull(typeInfo)) {
      throw BitSailException.asBitSailException(CommonErrorCode.CONVERT_NOT_SUPPORT, String
          .format("Primitive TypeInformation %s has no bridge type info.", bridge));
    }
    return typeInfo;
  }
}
