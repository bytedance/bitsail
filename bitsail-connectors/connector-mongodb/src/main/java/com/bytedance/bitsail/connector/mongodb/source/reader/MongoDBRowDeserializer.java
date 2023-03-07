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

package com.bytedance.bitsail.connector.mongodb.source.reader;

import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.common.typeinfo.BasicArrayTypeInfo;
import com.bytedance.bitsail.common.typeinfo.BasicTypeInfo;
import com.bytedance.bitsail.common.typeinfo.ListTypeInfo;
import com.bytedance.bitsail.common.typeinfo.MapTypeInfo;
import com.bytedance.bitsail.common.typeinfo.RowTypeInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfo;
import com.bytedance.bitsail.connector.mongodb.error.MongoDBErrorCode;

import com.alibaba.fastjson.JSON;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.ObjectId;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Deserializer for row data of MongoDB.
 */
public class MongoDBRowDeserializer {

  private final List<FiledConverter> converters;
  private final int fieldSize;

  public MongoDBRowDeserializer(RowTypeInfo rowTypeInfo) {
    String[] fieldNames = rowTypeInfo.getFieldNames();
    TypeInfo<?>[] typeInfos = rowTypeInfo.getTypeInfos();
    this.fieldSize = typeInfos.length;
    this.converters = new ArrayList<>();
    for (int i = 0; i < fieldSize; ++i) {
      converters.add(initFieldConverter(typeInfos[i], fieldNames[i]));
    }
  }

  public Row convert(Document document) {
    Row row = new Row(fieldSize);
    try {
      for (int i = 0; i < fieldSize; ++i) {
        row.setField(i, converters.get(i).apply(document));
      }
    } catch (Exception e) {
      throw BitSailException.asBitSailException(MongoDBErrorCode.CONVERT_ERROR, e.getCause());
    }
    return row;
  }

  private FiledConverter initFieldConverter(TypeInfo<?> typeInfo, String fieldName) {
    return document -> {
      Object value = document.get(fieldName);
      if (value == null) {
        return null;
      }
      try {
        return parseObject(value, typeInfo);
      } catch (Exception e) {
        throw new RuntimeException(String.format("field %s cannot convert %s to type %s", fieldName, value, typeInfo));
      }
    };
  }

  private Object parseObject(Object item, TypeInfo<?> typeInfo) {
    // BasicTypeInfo/byte[]/SqlTimeTypeInfo
    if (typeInfo instanceof BasicTypeInfo || typeInfo instanceof BasicArrayTypeInfo) {
      return parseBasicTypeInfo(item, typeInfo);
    } else if (typeInfo instanceof ListTypeInfo) {
      return parseListTypeInfo(item, (ListTypeInfo<?>) typeInfo);
    } else if (typeInfo instanceof MapTypeInfo) {
      return parseMapTypeInfo(item, (MapTypeInfo<?, ?>) typeInfo);
    } else {
      throw BitSailException.asBitSailException(MongoDBErrorCode.UNSUPPORTED_TYPE, "TypeInfo: " + typeInfo);
    }
  }

  @SuppressWarnings("checkstyle:MagicNumber")
  private Object parseBasicTypeInfo(Object value, TypeInfo<?> typeInfo) {
    switch (typeInfo.toString()) {
      case "Void":
        return null;
      case "String":
        if (value instanceof Document) {
          return JSON.toJSONString(parseDocument((Document) value));
        } else if (value instanceof List) {
          return JSON.toJSONString(parseArray(value));
        }
        return parseBasicType(value).toString();
      case "Integer":
        return Integer.valueOf(value.toString());
      case "Long":
        return new BigDecimal(value.toString()).toBigInteger().longValue();
      case "Float":
        return Float.valueOf(value.toString());
      case "Double":
        return Double.valueOf(value.toString());
      case "Timestamp":
        if (value instanceof BsonTimestamp) {
          int result = ((BsonTimestamp) value).getTime();
          return new Timestamp(result * 1000L);
        } else if (value instanceof Date) {
          return new Timestamp(((Date) value).getTime());
        }
        return value;
      case "byte[]":
        return ((Binary) value).getData();
      default:
        return value;
    }
  }

  private List<Object> parseListTypeInfo(Object value, ListTypeInfo<?> typeInfo) {
    List<Object> result = new ArrayList<>();
    TypeInfo<?> elementTypeInfo = typeInfo.getElementTypeInfo();
    for (Object element : (List<?>) value) {
      Object v = parseObject(element, elementTypeInfo);
      result.add(v);
    }
    return result;
  }

  @SuppressWarnings("unchecked")
  private Map<Object, Object> parseMapTypeInfo(Object value, MapTypeInfo<?, ?> typeInfo) {
    Map<Object, Object> result = new HashMap<>();
    TypeInfo<?> keyTypeInfo = typeInfo.getKeyTypeInfo();
    TypeInfo<?> valueTypeInfo = typeInfo.getValueTypeInfo();
    Map<Object, Object> map = (Map<Object, Object>) value;
    for (Object key : map.keySet()) {
      Object k = parseObject(key, keyTypeInfo);
      Object v = parseObject(map.get(key), valueTypeInfo);
      result.put(k, v);
    }
    return result;
  }

  @SuppressWarnings("checkstyle:MagicNumber")
  private Object parseBasicType(Object value) {
    if (value instanceof ObjectId) {
      return ((ObjectId) value).toString();
    } else if (value instanceof BsonTimestamp) {
      int result = ((BsonTimestamp) value).getTime();
      return new Timestamp(result * 1000L);
    } else if (value instanceof Binary) {
      // org.bson.types.Binary to base64 encoded string.
      // Ref: https://stackoverflow.com/questions/9338989/what-does-the-0-mean-in-mongodbs-bindata0-e8menzzofymmd7wshdnrfjyek8m
      return new String(Base64.getEncoder().encode(((Binary) value).getData()));
    } else if (value instanceof Date) {
      return new Timestamp(((Date) value).getTime());
    } else {
      return value;
    }
  }

  private List<Object> parseArray(Object value) {
    List<Object> result = new ArrayList<>();
    for (Object element : (List<?>) value) {
      Object v;
      if (element instanceof Document) {
        v = parseDocument((Document) element);
      } else if (element instanceof List) {
        v = parseArray(element);
      } else {
        v = parseBasicType(element);
      }
      result.add(v);
    }
    return result;
  }

  private Map<String, Object> parseDocument(Document document) {
    Map<String, Object> map = new HashMap<>();
    for (String key : document.keySet()) {
      Object value = document.get(key);
      if (value instanceof List) {
        Object array = parseArray(value);
        map.put(key, array);
      } else if (value instanceof Document) {
        Object m = parseDocument((Document) value);
        map.put(key, m);
      } else {
        Object basic = parseBasicType(value);
        map.put(key, basic);
      }
    }
    return map;
  }

  /**
   * Interface of field converter
   */
  interface FiledConverter {
    /**
     * Convert the MongoDB document
     *
     * @param document MongoDB document
     * @return result after converting
     * @throws Exception any exception if thrown
     */
    Object apply(Document document) throws Exception;
  }

}
