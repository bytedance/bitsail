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

package com.bytedance.bitsail.flink.core.delagate.converter;

import com.bytedance.bitsail.base.serializer.RowSerializer;
import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.column.Column;
import com.bytedance.bitsail.common.column.ListColumn;
import com.bytedance.bitsail.common.column.MapColumn;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.exception.CommonErrorCode;
import com.bytedance.bitsail.common.typeinfo.BasicArrayTypeInfo;
import com.bytedance.bitsail.common.typeinfo.ListTypeInfo;
import com.bytedance.bitsail.common.typeinfo.MapTypeInfo;
import com.bytedance.bitsail.common.typeinfo.RowTypeInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfos;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.bytedance.bitsail.common.typeinfo.TypeInfos.STRING_TYPE_INFO;

/**
 * Created 2022/6/21
 */
public class FlinkRowConvertSerializer implements RowSerializer<Row> {
  private final RowTypeInfo rowTypeInfo;

  private final BitSailConfiguration commonConfiguration;

  public FlinkRowConvertSerializer(RowTypeInfo rowTypeInfo,
                                   BitSailConfiguration commonConfiguration) {
    this.rowTypeInfo = rowTypeInfo;
    this.commonConfiguration = commonConfiguration;
  }

  @Override
  public Row serialize(com.bytedance.bitsail.common.row.Row row) throws IOException {
    Object[] fields = row.getFields();
    int arity = ArrayUtils.getLength(fields);
    Row flinkRow = new Row(org.apache.flink.types.RowKind.fromByteValue(row.getKind().toByteValue()), arity);
    for (int index = 0; index < arity; index++) {
      TypeInfo<?> typeInfo = rowTypeInfo.getTypeInfos()[index];
      Object field = row.getField(index);
      if (field instanceof Column) {
        field = deserializeColumn((Column) field, typeInfo, rowTypeInfo.getFieldNames()[index]);
      } else {
        if (!compareValueTypeInfo(field, typeInfo)) {
          //todo transform
          throw BitSailException.asBitSailException(CommonErrorCode.CONVERT_NOT_SUPPORT,
              String.format("column %s type info %s not match with value type %s.",
                  rowTypeInfo.getFieldNames()[index], typeInfo, field.getClass()));
        }
      }
      flinkRow.setField(index, field);
    }
    return flinkRow;
  }

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

      return compareValueTypeInfo(list.get(0), elementTypeInfo);

    }
    return value.getClass().isAssignableFrom(typeInfo.getTypeClass());
  }

  @Override
  public com.bytedance.bitsail.common.row.Row deserialize(Row serialized) throws IOException {
    int arity = serialized.getArity();
    Object[] fields = new Object[arity];
    for (int index = 0; index < arity; index++) {
      TypeInfo<?> typeInfo = rowTypeInfo.getTypeInfos()[index];
      Object field = serialized.getField(index);
      String name = rowTypeInfo.getFieldNames()[index];
      if (field instanceof Column) {
        fields[index] = deserializeColumn((Column) field, typeInfo, name);
      } else {
        fields[index] = field;
      }
    }
    return new com.bytedance.bitsail.common.row.Row(
        serialized.getKind().toByteValue(),
        fields);
  }

  private Object deserializeColumn(Column object, TypeInfo<?> typeInfo, String name) throws BitSailException {
    if (Objects.isNull(object)) {
      return null;
    }

    Class<?> typeInfoTypeClass = typeInfo.getTypeClass();
    if (List.class.isAssignableFrom(typeInfoTypeClass)) {
      if (!(object instanceof ListColumn)) {
        throw BitSailException.asBitSailException(CommonErrorCode.CONVERT_NOT_SUPPORT,
            String.format("Column %s is not list type, value: %s", name, object));
      }
      return getListColumnValue((List<Column>) object, (ListTypeInfo<?>) typeInfo, name);
    }

    if (Map.class.isAssignableFrom(typeInfoTypeClass)) {
      if (!(object instanceof MapColumn)) {
        throw BitSailException.asBitSailException(CommonErrorCode.CONVERT_NOT_SUPPORT,
            String.format("Column %s is not map type, value: %s", name, object));
      }
      return getMapColumnValue((MapColumn<Column, Column>) object, (MapTypeInfo<?, ?>) typeInfo, name);
    }

    return getBasicTypeColumnValue((Column) object, typeInfo);
  }

  private List<?> getListColumnValue(List<Column> columns, ListTypeInfo<?> listTypeInfo, String name) {
    TypeInfo<?> elementTypeInfo = listTypeInfo.getElementTypeInfo();
    List<Object> objects = new ArrayList<>();
    if (Objects.nonNull(columns)) {
      for (Column column : columns) {
        objects.add(deserializeColumn(column, elementTypeInfo, name));
      }
    }
    return objects;
  }

  private Map<?, ?> getMapColumnValue(Map<Column, Column> columnMap, MapTypeInfo<?, ?> mapTypeInfo, String name) {
    TypeInfo<?> keyTypeInfo = mapTypeInfo.getKeyTypeInfo();
    TypeInfo<?> valueTypeInfo = mapTypeInfo.getValueTypeInfo();

    Map<Object, Object> maps = new HashMap<>();
    if (Objects.nonNull(columnMap)) {
      columnMap.forEach((key, value) -> {
        Object keyValue = deserializeColumn(key, keyTypeInfo, name);
        if (Objects.isNull(keyValue)) {
          throw new BitSailException(CommonErrorCode.CONVERT_NOT_SUPPORT, "");
        }
        Object mapValue = deserializeColumn(value, valueTypeInfo, name);
        maps.put(keyValue, mapValue);
      });
    }
    return maps;
  }

  private Object getBasicTypeColumnValue(Column column, TypeInfo<?> typeInfo) {
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

}
