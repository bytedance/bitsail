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

package com.bytedance.bitsail.common.type;

import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.ddl.typeinfo.ListTypeInfo;
import com.bytedance.bitsail.common.ddl.typeinfo.MapTypeInfo;
import com.bytedance.bitsail.common.ddl.typeinfo.TypeInfo;
import com.bytedance.bitsail.common.exception.CommonErrorCode;

/**
 * Created 2022/5/11
 */
public class JdbcTypeInfoConverter extends BaseEngineTypeInfoConverter {
  private static final String LIST_TYPE_NAME = "list";
  private static final String MAP_TYPE_NAME = "map";

  public JdbcTypeInfoConverter(String engineName) {
    super(engineName);
  }

  private static boolean isArrayType(String engineType) {
    return engineType.startsWith(LIST_TYPE_NAME);
  }

  public static boolean isMapType(String engineType) {
    return engineType.startsWith(MAP_TYPE_NAME);
  }

  @Override
  public TypeInfo<?> toTypeInfo(String engineType) {
    engineType = getBaseName(engineType);
    if (isArrayType(engineType)) {
      return getArrayTypeInfo(engineType);

    } else if (isMapType(engineType)) {
      return getMapTypeInfo(engineType);

    } else {
      return reader.getToTypeInformation().get(engineType);
    }
  }

  public TypeInfo<?> getArrayTypeInfo(String engineType) {
    if (!engineType.endsWith(">")) {
      throw BitSailException.asBitSailException(CommonErrorCode.CONVERT_NOT_SUPPORT,
          "Source engine " + engineName + ", invalid engine list type: " + engineType);
    }
    String elementType = engineType.substring(LIST_TYPE_NAME.length() + 1, engineType.length() - 1);
    TypeInfo<?> elementTypeInfo = toTypeInfo(elementType);
    return new ListTypeInfo<>(elementTypeInfo);
  }

  public TypeInfo<?> getMapTypeInfo(String engineType) {
    if (!engineType.endsWith(">") || !engineType.contains(",")) {
      throw BitSailException.asBitSailException(CommonErrorCode.CONVERT_NOT_SUPPORT,
          "Source engine " + engineName + ", invalid engine map type: " + engineType);
    }
    String subString = engineType.substring(MAP_TYPE_NAME.length() + 1, engineType.length() - 1);
    String[] parts = subString.split(",", 2);
    String keyType = parts[0];
    String valueType = parts[1];

    TypeInfo<?> keyTypeInformation = toTypeInfo(keyType);
    TypeInfo<?> valueTypeInformation = toTypeInfo(valueType);
    return new MapTypeInfo<>(keyTypeInformation, valueTypeInformation);

  }
}
