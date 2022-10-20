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

package com.bytedance.bitsail.common.ddl.typeinfo;

import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

@Getter
public enum PrimitiveTypes {

  VOID("void", TypeFactory.VOID_TYPE_INFO),

  SHORT("short", TypeFactory.SHORT_TYPE_INFO),

  INT("int", TypeFactory.INT_TYPE_INFO),

  LONG("long", TypeFactory.LONG_TYPE_INFO),

  BIGINT("bigint", TypeFactory.LONG_TYPE_INFO),

  FLOAT("float", TypeFactory.FLOAT_TYPE_INFO),

  DOUBLE("double", TypeFactory.DOUBLE_TYPE_INFO),

  BIG_DECIMAL("bigdecimal", TypeFactory.BIG_DEC_TYPE_INFO),

  BYTE("byte", TypeFactory.BYTE_TYPE_INFO),

  BINARY("bytes", TypeFactory.BYTE_PRIMITIVE_ARRAY_TYPE_INFO),

  STRING("string", TypeFactory.STRING_TYPE_INFO),

  BOOLEAN("boolean", TypeFactory.BOOLEAN_TYPE_INFO),

  DATE_DATE("date.date", TypeFactory.DATE),

  DATE_TIME("date.time", TypeFactory.TIME),

  DATE_DATE_TIME("date.datetime", TypeFactory.TIMESTAMP);

  public static final Map<String, TypeInfo<?>> TYPE_STORE = Arrays
      .stream(PrimitiveTypes.values())
      .collect(Collectors.toMap(PrimitiveTypes::getCanonicalName, PrimitiveTypes::getTypeInfo));
  private static final String LIST_TYPE_NAME = "list";
  private static final String MAP_TYPE_NAME = "map";
  /**
   * TypeInformation name
   */
  private final String canonicalName;
  /**
   * TypeInformation
   */
  private final TypeInfo<?> typeInfo;

  PrimitiveTypes(String canonicalName, TypeInfo<?> typeInfo) {
    this.canonicalName = canonicalName;
    this.typeInfo = typeInfo;
  }

  public static boolean isPrimitiveType(String type) {
    return TYPE_STORE.containsKey(type);
  }

  public static boolean isArrayType(String type) {
    return StringUtils.startsWith(type, LIST_TYPE_NAME);
  }

  public static String getArrayElementType(String customType) {
    return StringUtils.trim((StringUtils.substring(customType, LIST_TYPE_NAME.length() + 1,
        customType.length() - 1)));
  }

  public static boolean isMapType(String type) {
    return StringUtils.startsWith(type, MAP_TYPE_NAME);
  }

  public static String[] getMapKeyValueType(String customType) {
    return StringUtils.split(StringUtils
        .substring(customType, MAP_TYPE_NAME.length() + 1, customType.length() - 1), ",", 2);
  }
}
