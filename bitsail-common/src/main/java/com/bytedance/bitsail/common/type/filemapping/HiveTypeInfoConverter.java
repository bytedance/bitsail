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

package com.bytedance.bitsail.common.type.filemapping;

import com.bytedance.bitsail.common.typeinfo.ListTypeInfo;
import com.bytedance.bitsail.common.typeinfo.MapTypeInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfo;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created 2022/5/11
 */
public class HiveTypeInfoConverter extends FileMappingTypeInfoConverter {
  private static final Logger LOG = LoggerFactory.getLogger(HiveTypeInfoConverter.class);

  public static final String DEFAULT_STAGE_NAME = "hive";
  private static final String LIST_TYPE_NAME = "array";
  private static final String MAP_TYPE_NAME = "map";

  public HiveTypeInfoConverter() {
    super(DEFAULT_STAGE_NAME);
  }

  public HiveTypeInfoConverter(String engineName) {
    super(engineName);
  }

  private static boolean isArrayType(String engineType) {
    return StringUtils.startsWithIgnoreCase(engineType, LIST_TYPE_NAME);
  }

  private static void expect(String engineType, int index, char expect) {
    Preconditions.checkArgument(engineType.charAt(index) == expect,
        String.format("Engine type: %s, expect token is %s in index %s, but actually is %s",
            engineType, expect, index, engineType.charAt(index)));
  }

  @Override
  public TypeInfo<?> fromTypeString(String typeString) {
    Preconditions.checkNotNull(typeString,
        String.format("Type string %s can not be null.", typeString));
    typeString = trim(getBaseName(typeString));
    LOG.debug("type string = {}.", typeString);
    if (isArrayType(typeString)) {
      return internalRecursion(typeString, Category.LIST);
    }

    if (isMapType(typeString)) {
      return internalRecursion(typeString, Category.MAP);
    }

    return internalRecursion(typeString, Category.PRIMITIVE);
  }

  @Override
  public String fromTypeInfo(TypeInfo<?> typeInfo) {
    return reader.getFromTypeInformation().get(typeInfo);
  }

  private boolean isMapType(String engineType) {
    return StringUtils.startsWithIgnoreCase(engineType, MAP_TYPE_NAME);
  }

  private TypeInfo<?> internalRecursion(String engineType, Category category) {
    switch (category) {
      case PRIMITIVE:
        String typeInfoTypeName = getBaseName(engineType);
        return reader.getToTypeInformation().get(typeInfoTypeName);

      case LIST:
        expect(engineType, LIST_TYPE_NAME.length(), '<');
        expect(engineType, engineType.length() - 1, '>');

        String elementType = trim(StringUtils.substring(engineType, LIST_TYPE_NAME.length() + 1,
            engineType.length() - 1));
        return new ListTypeInfo<>(fromTypeString(elementType));

      case MAP:
        expect(engineType, MAP_TYPE_NAME.length(), '<');
        expect(engineType, engineType.length() - 1, '>');
        String[] split = StringUtils.split(StringUtils
            .substring(engineType, MAP_TYPE_NAME.length() + 1, engineType.length() - 1), ",", 2);

        String keyType = trim(split[0]);
        String valueType = trim(split[1]);
        return new MapTypeInfo<>(fromTypeString(keyType), fromTypeString(valueType));

      default:
        throw new IllegalArgumentException(String.format("Non type match for the type: %s.", engineType));
    }
  }

  private enum Category {
    PRIMITIVE,
    LIST,
    MAP,
    //todo
    STRUCT,
    UNION
  }

}
