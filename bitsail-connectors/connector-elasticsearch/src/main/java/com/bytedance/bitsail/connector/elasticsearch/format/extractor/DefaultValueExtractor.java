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

package com.bytedance.bitsail.connector.elasticsearch.format.extractor;

import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.common.typeinfo.RowTypeInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfoValueConverter;
import com.bytedance.bitsail.common.typeinfo.TypeInfos;
import com.bytedance.bitsail.common.util.JsonSerializer;

import com.google.common.collect.Maps;

import java.io.Serializable;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

public class DefaultValueExtractor implements Function<Row, String>, Serializable {

  private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter
      .ofPattern("yyyy-MM-dd");

  private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter
      .ofPattern("yyyy-MM-dd HH:mm:ss");

  private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter
      .ofPattern("HH:mm:ss");

  private final RowTypeInfo rowTypeInfo;

  //TODO use value converter to convert value to string.
  private final TypeInfoValueConverter valueConverter;

  public DefaultValueExtractor(TypeInfoValueConverter valueConverter,
                               RowTypeInfo rowTypeInfo) {
    this.valueConverter = valueConverter;
    this.rowTypeInfo = rowTypeInfo;
  }

  @Override
  public String apply(Row row) {
    Map<String, Object> document = Maps.newHashMapWithExpectedSize(row.getArity());
    for (int index = 0; index < row.getArity(); index++) {
      String name = rowTypeInfo.getFieldNames()[index];
      Object value = convert(row.getField(index), rowTypeInfo.getTypeInfos()[index]);
      document.put(name, value);
    }
    return JsonSerializer.serialize(document);
  }

  public Object convert(Object value, TypeInfo<?> typeInfo) {
    if (Objects.isNull(value)) {
      return null;
    }
    Class<?> typeClass = typeInfo.getTypeClass();
    if (TypeInfos.VOID_TYPE_INFO.getTypeClass() == typeClass) {
      return null;
    }

    if (TypeInfos.LOCAL_DATE_TYPE_INFO.getTypeClass() == typeClass) {
      return ((LocalDate) value)
          .format(DATE_FORMATTER);
    }

    if (TypeInfos.LOCAL_DATE_TIME_TYPE_INFO.getTypeClass() == typeClass) {
      return ((LocalDateTime) value)
          .format(DATE_TIME_FORMATTER);
    }

    if (TypeInfos.LOCAL_TIME_TYPE_INFO.getTypeClass() == typeClass) {
      return ((LocalTime) value)
          .format(TIME_FORMATTER);
    }

    if (TypeInfos.SQL_TIMESTAMP_TYPE_INFO.getTypeClass() == typeClass) {
      LocalDateTime localDateTime = ((Timestamp) value).toLocalDateTime();
      return localDateTime.format(DATE_TIME_FORMATTER);
    }

    if (TypeInfos.SQL_DATE_TYPE_INFO.getTypeClass() == typeClass) {
      Date date = (java.sql.Date) value;
      return date.toLocalDate()
          .format(DATE_FORMATTER);
    }

    if (TypeInfos.SQL_TIME_TYPE_INFO.getTypeClass() == typeClass) {
      Time time = (java.sql.Time) value;
      return time.toLocalTime()
          .format(TIME_FORMATTER);
    }

    return value;
  }

}
