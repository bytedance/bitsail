/*
 * Copyright 2022-2023 Bytedance Ltd. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.bytedance.bitsail.component.format.json;

import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.common.typeinfo.BasicArrayTypeInfo;
import com.bytedance.bitsail.common.typeinfo.RowTypeInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfos;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;

/**
 * Convert BitSail Row into Json.
 */
public class RowToJsonConverter implements Serializable {
  private static final long serialVersionUID = 1L;

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final int arity;
  private final String[] fieldNames;
  private final JsonConverter[] fieldConverters;

  public RowToJsonConverter(RowTypeInfo rowTypeInfo) {
    assert rowTypeInfo.getTypeInfos().length == rowTypeInfo.getFieldNames().length;
    this.arity = rowTypeInfo.getTypeInfos().length;
    this.fieldNames = rowTypeInfo.getFieldNames();
    this.fieldConverters = Arrays.stream(rowTypeInfo.getTypeInfos())
        .map(this::createConverter).toArray(JsonConverter[]::new);
  }

  public JsonNode convert(Row row) {
    ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
    String columnName;
    for (int i = 0; i < arity; i++) {
      columnName = fieldNames[i];
      objectNode.set(columnName, fieldConverters[i].convert(OBJECT_MAPPER, row.getField(i)));
    }
    return objectNode;
  }

  public interface JsonConverter extends Serializable {
    JsonNode convert(ObjectMapper mapper, Object value);
  }

  private JsonConverter createConverter(TypeInfo<?> typeInfo) {
    Class<?> typeClass = typeInfo.getTypeClass();

    if (typeClass == TypeInfos.VOID_TYPE_INFO.getTypeClass()) {
      return (mapper, value) -> mapper.getNodeFactory().nullNode();
    }
    if (typeClass == TypeInfos.BOOLEAN_TYPE_INFO.getTypeClass()) {
      return (mapper, value) ->
          mapper.getNodeFactory().booleanNode((boolean) value);
    }
    if (typeClass == TypeInfos.SHORT_TYPE_INFO.getTypeClass()) {
      return (mapper, value) -> mapper.getNodeFactory().numberNode((short) value);
    }
    if (typeClass == TypeInfos.INT_TYPE_INFO.getTypeClass()) {
      return (mapper, value) -> mapper.getNodeFactory().numberNode((int) value);
    }
    if (typeClass == TypeInfos.LONG_TYPE_INFO.getTypeClass()) {
      return (mapper, value) -> mapper.getNodeFactory().numberNode((long) value);
    }
    if (typeClass == TypeInfos.BIG_INTEGER_TYPE_INFO.getTypeClass()) {
      return (mapper, value) -> mapper.getNodeFactory().numberNode((BigInteger) value);
    }
    if (typeClass == TypeInfos.FLOAT_TYPE_INFO.getTypeClass()) {
      return (mapper, value) -> mapper.getNodeFactory().numberNode((float) value);
    }
    if (typeClass == TypeInfos.DOUBLE_TYPE_INFO.getTypeClass()) {
      return (mapper, value) -> mapper.getNodeFactory().numberNode((double) value);
    }
    if (typeClass == TypeInfos.BIG_DECIMAL_TYPE_INFO.getTypeClass()) {
      return (mapper, value) -> mapper.getNodeFactory().numberNode(new BigDecimal(value.toString()));
    }
    if (typeClass == TypeInfos.STRING_TYPE_INFO.getTypeClass()) {
      return createStringConverter();
    }
    if (typeClass == TypeInfos.LOCAL_DATE_TIME_TYPE_INFO.getTypeClass()) {
      return createStringConverter();
    }
    if (typeClass == TypeInfos.LOCAL_DATE_TYPE_INFO.getTypeClass()) {
      return createStringConverter();
    }
    if (typeClass == TypeInfos.LOCAL_TIME_TYPE_INFO.getTypeClass()) {
      return createStringConverter();
    }
    if (typeClass == BasicArrayTypeInfo.BINARY_TYPE_INFO.getTypeClass()) {
      return (mapper, value) -> mapper.getNodeFactory().binaryNode((byte[]) value);
    }
    return createStringConverter();
  }

  private JsonConverter createStringConverter() {
    return (mapper, value) -> mapper.getNodeFactory().textNode(value.toString());
  }

}
