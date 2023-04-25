/*
 *       Copyright 2022-2023 Bytedance Ltd. and/or its affiliates.
 *
 *       Licensed under the Apache License, Version 2.0 (the "License");
 *       you may not use this file except in compliance with the License.
 *       You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *       Unless required by applicable law or agreed to in writing, software
 *       distributed under the License is distributed on an "AS IS" BASIS,
 *       WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *       See the License for the specific language governing permissions and
 *       limitations under the License.
 */

package com.bytedance.bitsail.common.typeinfo;

import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.column.Column;
import com.bytedance.bitsail.common.column.ListColumn;
import com.bytedance.bitsail.common.column.MapColumn;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.exception.CommonErrorCode;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.bytedance.bitsail.common.typeinfo.TypeInfos.STRING_TYPE_INFO;

public class TypeInfoValueConverter implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(TypeInfoValueConverter.class);

  private TypeInfoCompatibles typeInfoCompatibles;

  public TypeInfoValueConverter(BitSailConfiguration commonConfiguration) {
    this.typeInfoCompatibles = new TypeInfoCompatibles(commonConfiguration);
  }

  /**
   * Try to convert value to type info's definition.
   */
  public Object convertObject(Object value,
                              TypeInfo<?> typeInfo) {
    //Return null directly if input is null.
    if (Objects.isNull(value)) {
      return null;
    }

    if (value instanceof Column) {
      return convertColumnObject((Column) value, typeInfo);
    }

    if (compareValueTypeInfo(value, typeInfo)) {
      return value;
    }
    return convertJavaObject(value, typeInfo);
  }

  private Object convertColumnObject(Column value,
                                     TypeInfo<?> typeInfo) {
    if (Objects.isNull(value)) {
      return null;
    }

    Class<?> typeInfoTypeClass = typeInfo.getTypeClass();
    if (List.class.isAssignableFrom(typeInfoTypeClass)) {
      if (!(value instanceof ListColumn)) {
        throw BitSailException.asBitSailException(CommonErrorCode.CONVERT_NOT_SUPPORT,
            String.format("Column is not list column type, value: %s", value));
      }
      return convertListColumnObject((ListColumn<?>) value, (ListTypeInfo<?>) typeInfo);
    }

    if (Map.class.isAssignableFrom(typeInfoTypeClass)) {
      if (!(value instanceof MapColumn)) {
        throw BitSailException.asBitSailException(CommonErrorCode.CONVERT_NOT_SUPPORT,
            String.format("Column is not map column type, value: %s", value));
      }
      return convertMapColumnObject((MapColumn<?, ?>) value, (MapTypeInfo<?, ?>) typeInfo);
    }

    return convertPrimitiveColumnObject(value, typeInfo);
  }

  private List<?> convertListColumnObject(ListColumn<?> columns, ListTypeInfo<?> listTypeInfo) {
    TypeInfo<?> elementTypeInfo = listTypeInfo.getElementTypeInfo();
    List<Object> objects = new ArrayList<>();
    if (Objects.nonNull(columns)) {
      for (Column column : columns) {
        objects.add(convertColumnObject(column, elementTypeInfo));
      }
    }
    return objects;
  }

  private Map<?, ?> convertMapColumnObject(Map<?, ?> columnMap, MapTypeInfo<?, ?> mapTypeInfo) {
    TypeInfo<?> keyTypeInfo = mapTypeInfo.getKeyTypeInfo();
    TypeInfo<?> valueTypeInfo = mapTypeInfo.getValueTypeInfo();

    Map<Object, Object> maps = new HashMap<>();
    if (Objects.nonNull(columnMap)) {
      columnMap.forEach((key, value) -> {
        Object keyValue = convertColumnObject((Column) key, keyTypeInfo);
        if (Objects.isNull(keyValue)) {
          throw new BitSailException(CommonErrorCode.CONVERT_NOT_SUPPORT, "Map's key can't be null.");
        }
        Object mapValue = convertColumnObject((Column) value, valueTypeInfo);
        maps.put(keyValue, mapValue);
      });
    }
    return maps;
  }

  private Object convertPrimitiveColumnObject(Column column, TypeInfo<?> typeInfo) {
    Class<?> typeInfoTypeClass = typeInfo.getTypeClass();
    if (null == column.getRawData()) {
      return null;
    }

    if (STRING_TYPE_INFO.getTypeClass() == typeInfoTypeClass) {
      return column.asString();
    } else if (TypeInfos.BOOLEAN_TYPE_INFO.getTypeClass() == typeInfoTypeClass) {
      return column.asBoolean();
    } else if (TypeInfos.BYTE_TYPE_INFO.getTypeClass() == typeInfoTypeClass) {
      return column.asLong().byteValue();
    } else if (TypeInfos.INT_TYPE_INFO.getTypeClass() == typeInfoTypeClass) {
      return column.asLong().intValue();
    } else if (TypeInfos.SHORT_TYPE_INFO.getTypeClass() == typeInfoTypeClass) {
      return column.asLong().shortValue();
    } else if (TypeInfos.LONG_TYPE_INFO.getTypeClass() == typeInfoTypeClass) {
      return column.asLong();
    } else if (TypeInfos.BIG_INTEGER_TYPE_INFO.getTypeClass() == typeInfoTypeClass) {
      return column.asBigInteger();
    } else if (TypeInfos.FLOAT_TYPE_INFO.getTypeClass() == typeInfoTypeClass) {
      return column.asDouble().floatValue();
    } else if (TypeInfos.DOUBLE_TYPE_INFO.getTypeClass() == typeInfoTypeClass) {
      return column.asDouble();
    } else if (TypeInfos.BIG_DECIMAL_TYPE_INFO.getTypeClass() == typeInfoTypeClass) {
      return column.asBigDecimal();
    } else if (TypeInfos.SQL_DATE_TYPE_INFO.getTypeClass() == typeInfoTypeClass) {
      return new java.sql.Date(column.asDate().getTime());
    } else if (TypeInfos.SQL_TIME_TYPE_INFO.getTypeClass() == typeInfoTypeClass) {
      return new java.sql.Time(column.asDate().getTime());
    } else if (TypeInfos.SQL_TIMESTAMP_TYPE_INFO.getTypeClass() == typeInfoTypeClass) {
      return new java.sql.Timestamp(column.asDate().getTime());
    } else if (TypeInfos.LOCAL_DATE_TYPE_INFO.getTypeClass() == typeInfoTypeClass) {
      return column.asDate();
    } else if (TypeInfos.LOCAL_TIME_TYPE_INFO.getTypeClass() == typeInfoTypeClass) {
      return column.asDate();
    } else if (TypeInfos.LOCAL_DATE_TIME_TYPE_INFO.getTypeClass() == typeInfoTypeClass) {
      return column.asDate();
    } else if (BasicArrayTypeInfo.BINARY_TYPE_INFO.getTypeClass() == typeInfoTypeClass) {
      return column.asBytes();
    } else {
      throw BitSailException.asBitSailException(CommonErrorCode.CONVERT_NOT_SUPPORT,
          "Flink basic data type " + typeInfoTypeClass + " is not supported!");
    }
  }

  /**
   * Compare object's value type match with type info's definition or not.
   */
  private static boolean compareValueTypeInfo(Object value,
                                              TypeInfo<?> typeInfo) {
    if (Objects.isNull(value)) {
      return true;
    }

    if (typeInfo instanceof MapTypeInfo) {
      if (!(value instanceof Map)) {
        return false;
      }
      Map<?, ?> map = (Map<?, ?>) value;
      if (MapUtils.isEmpty(map)) {
        return true;
      }

      MapTypeInfo<?, ?> mapTypeInfo = (MapTypeInfo<?, ?>) typeInfo;
      TypeInfo<?> keyTypeInfo = mapTypeInfo.getKeyTypeInfo();
      TypeInfo<?> valueTypeInfo = mapTypeInfo.getValueTypeInfo();

      Iterator<?> keyIterator = map.keySet().iterator();
      Object next = keyIterator.next();

      //TODO find first not null key and first not null value, maybe it will consumer more resources.
      return compareValueTypeInfo(next, keyTypeInfo)
          && compareValueTypeInfo(map.get(next), valueTypeInfo);
    }

    if (typeInfo instanceof ListTypeInfo) {
      if ((!(value instanceof List))) {
        return false;
      }
      List<?> list = (List<?>) value;
      if (CollectionUtils.isEmpty(list)) {
        return true;
      }
      ListTypeInfo<?> listTypeInfo = (ListTypeInfo<?>) typeInfo;
      TypeInfo<?> elementTypeInfo = listTypeInfo.getElementTypeInfo();

      //TODO find first not null element, maybe it will consumer more resources.
      return compareValueTypeInfo(list.get(0), elementTypeInfo);

    }
    return value.getClass().isAssignableFrom(typeInfo.getTypeClass());
  }

  private Object convertJavaObject(Object value, TypeInfo<?> typeInfo) {
    if (Objects.isNull(value)) {
      return null;
    }

    if (typeInfo instanceof MapTypeInfo) {
      if (!(value instanceof Map)) {
        throw BitSailException.asBitSailException(CommonErrorCode.CONVERT_NOT_SUPPORT,
            String.format("Type %s can't convert to map type.", value.getClass()));
      }
      MapTypeInfo<?, ?> mapTypeInfo = (MapTypeInfo<?, ?>) typeInfo;
      Map<?, ?> origin = (Map<?, ?>) value;
      Map<Object, Object> converted = Maps.newHashMap();
      for (Object key : origin.keySet()) {
        converted.put(convertJavaObject(key, mapTypeInfo.getKeyTypeInfo()),
            convertJavaObject(origin.get(key), mapTypeInfo.getValueTypeInfo()));
      }
      return converted;
    } else if (typeInfo instanceof ListTypeInfo) {
      if (!(value instanceof List)) {
        throw BitSailException.asBitSailException(CommonErrorCode.CONVERT_NOT_SUPPORT,
            String.format("Type %s can't convert to list type.", value.getClass()));
      }
      ListTypeInfo<?> listTypeInfo = (ListTypeInfo<?>) typeInfo;
      List<?> origin = (List<?>) value;
      List<Object> converted = Lists.newArrayList();
      for (Object key : origin) {
        converted.add(convertJavaObject(key, listTypeInfo.getElementTypeInfo()));
      }
      return converted;

    } else {
      return convertPrimitiveObject(value, typeInfo);
    }
  }

  /**
   * TODO add chart to show the relation of the type conversion.
   * TODO check number type overflow when do the convert.
   */
  private Object convertPrimitiveObject(Object value, TypeInfo<?> typeInfo) {
    if (Objects.isNull(value)) {
      return null;
    }

    TypeInfo<?> valueTypeInfo = TypeInfoBridge.bridgeTypeClass(value.getClass());
    if (value.getClass() == typeInfo.getTypeClass()) {
      return value;
    }

    if (!(valueTypeInfo instanceof BasicTypeInfo)) {
      return valueTypeInfo.compatibleTo(typeInfo, value);
    }
    return typeInfoCompatibles.compatibleTo(valueTypeInfo, typeInfo, value);
  }
}
