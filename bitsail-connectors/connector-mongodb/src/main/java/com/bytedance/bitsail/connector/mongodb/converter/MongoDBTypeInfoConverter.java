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

package com.bytedance.bitsail.connector.mongodb.converter;

import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.exception.CommonErrorCode;
import com.bytedance.bitsail.common.type.filemapping.FileMappingTypeInfoConverter;
import com.bytedance.bitsail.common.typeinfo.ListTypeInfo;
import com.bytedance.bitsail.common.typeinfo.MapTypeInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfos;
import com.bytedance.bitsail.common.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Type converter for MongoDB
 */
public class MongoDBTypeInfoConverter extends FileMappingTypeInfoConverter {

  private static final Logger LOG = LoggerFactory.getLogger(MongoDBTypeInfoConverter.class);

  private static final String OBJECT_TYPE = "object";
  private static final String ARRAY_TYPE = "array";
  private static final String GREATER_SIGN = ">";
  private static final String SPLIT_COMMA = ",";

  public MongoDBTypeInfoConverter() {
    super("mongodb");
  }

  @Override
  public TypeInfo<?> fromTypeString(String typeString) {
    Preconditions.checkNotNull(typeString, String.format("Type string %s cannot be null or empty", typeString));

    return getTypeInfo(typeString);
  }

  private TypeInfo<?> getTypeInfo(String typeString) {
    TypeInfo<?> typeInfo;
    typeString = FileMappingTypeInfoConverter.trim(typeString);
    if (isBasicType(typeString)) {
      typeInfo = getBasicTypeInfoFromMongoType(typeString);
    } else if (isArrayType(typeString)) {
      typeInfo = getListTypeInfoFromMongoType(typeString);
    } else if (isObjectType(typeString)) {
      typeInfo = getMapTypeInfoFromMongoType(typeString);
    } else {
      throw BitSailException.asBitSailException(CommonErrorCode.CONVERT_NOT_SUPPORT,
          "Invalid MongoDB type: " + typeString);
    }

    LOG.debug("MongoDB type [{}] converted to [{}]", typeString, typeInfo.getTypeClass().getName());
    return typeInfo;
  }

  private boolean isBasicType(String mongoType) {
    return reader.getToTypeInformation().containsKey(mongoType);
  }

  private boolean isArrayType(String mongoType) {
    return mongoType.startsWith(ARRAY_TYPE);
  }

  private boolean isObjectType(String mongoType) {
    return mongoType.startsWith(OBJECT_TYPE);
  }

  private TypeInfo<?> getBasicTypeInfoFromMongoType(String mongoType) {
    return reader.getToTypeInformation().get(mongoType);
  }

  public TypeInfo<?> getListTypeInfoFromMongoType(String type) {
    if (type.equals(ARRAY_TYPE)) {
      return new ListTypeInfo<>(TypeInfos.STRING_TYPE_INFO);
    }
    if (!type.endsWith(GREATER_SIGN)) {
      throw BitSailException.asBitSailException(CommonErrorCode.CONVERT_NOT_SUPPORT,
          "Invalid MongoDB array type: " + type);
    }
    String elementType = type.substring(ARRAY_TYPE.length() + 1, type.length() - 1);
    TypeInfo<?> elementTypeInformation = getTypeInfo(elementType);
    return new ListTypeInfo<>(elementTypeInformation);
  }

  public TypeInfo<?> getMapTypeInfoFromMongoType(String type) {
    if (type.equals(OBJECT_TYPE)) {
      return new MapTypeInfo<>(TypeInfos.STRING_TYPE_INFO, TypeInfos.STRING_TYPE_INFO);
    }
    if (!type.endsWith(GREATER_SIGN) || !type.contains(SPLIT_COMMA)) {
      throw BitSailException.asBitSailException(CommonErrorCode.CONVERT_NOT_SUPPORT,
          "Invalid MongoDB map type: " + type);
    }
    String subString = type.substring(OBJECT_TYPE.length() + 1, type.length() - 1);
    String[] parts = subString.split(SPLIT_COMMA, 2);
    String keyType = parts[0];
    String valueType = parts[1];

    TypeInfo<?> keyTypeInformation = getBasicTypeInfoFromMongoType(keyType);
    TypeInfo<?> valueTypeInformation = getTypeInfo(valueType);
    return new MapTypeInfo<>(keyTypeInformation, valueTypeInformation);
  }

}

