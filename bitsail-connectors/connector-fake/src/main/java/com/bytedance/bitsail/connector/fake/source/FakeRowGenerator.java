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

package com.bytedance.bitsail.connector.fake.source;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.common.typeinfo.BasicArrayTypeInfo;
import com.bytedance.bitsail.common.typeinfo.BasicTypeInfo;
import com.bytedance.bitsail.common.typeinfo.ListTypeInfo;
import com.bytedance.bitsail.common.typeinfo.MapTypeInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfos;
import com.bytedance.bitsail.common.typeinfo.TypeProperty;
import com.bytedance.bitsail.connector.fake.option.FakeReaderOptions;

import cn.ipokerface.snowflake.SnowflakeIdGenerator;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import net.datafaker.Faker;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Map;

import static com.bytedance.bitsail.common.typeinfo.TypeProperty.NULLABLE;

public class FakeRowGenerator {

  private final transient Faker faker;
  private final transient SnowflakeIdGenerator snowflakeIdGenerator;

  private final Integer nullPercentage;
  private final long upper;
  private final long lower;
  private final transient Timestamp fromTimestamp;
  private final transient Timestamp toTimestamp;

  public FakeRowGenerator(BitSailConfiguration jobConf, int taskId) {
    this.faker = new Faker();
    this.snowflakeIdGenerator = new SnowflakeIdGenerator(taskId, taskId);

    this.nullPercentage = jobConf.get(FakeReaderOptions.NULL_PERCENTAGE);
    this.upper = jobConf.get(FakeReaderOptions.UPPER_LIMIT);
    this.lower = jobConf.get(FakeReaderOptions.LOWER_LIMIT);
    this.fromTimestamp = Timestamp.valueOf(jobConf.get(FakeReaderOptions.FROM_TIMESTAMP));
    this.toTimestamp = Timestamp.valueOf(jobConf.get(FakeReaderOptions.TO_TIMESTAMP));
  }

  public Row fakeOneRecord(TypeInfo<?>[] typeInfos) {
    Row row = new Row(ArrayUtils.getLength(typeInfos));
    for (int index = 0; index < typeInfos.length; index++) {
      TypeInfo<?> typeInfo = typeInfos[index];
      if (isNullable(typeInfo) && isNull()) {
        row.setField(index, null);
      } else {
        row.setField(index, fakeRawValue(typeInfo));
      }
    }
    return row;
  }

  private boolean isNullable(TypeInfo<?> typeInfo) {
    return typeInfo instanceof BasicTypeInfo
        && CollectionUtils.isNotEmpty(typeInfo.getTypeProperties())
        && typeInfo.getTypeProperties().contains(NULLABLE);
  }

  @SuppressWarnings("checkstyle:MagicNumber")
  private boolean isNull() {
    return (faker.number().randomNumber() % 100) < nullPercentage;
  }

  @SuppressWarnings("checkstyle:MagicNumber")
  private Object fakeRawValue(TypeInfo<?> typeInfo) {

    if (TypeInfos.LONG_TYPE_INFO.getTypeClass() == typeInfo.getTypeClass()) {
      if (CollectionUtils.isNotEmpty(typeInfo.getTypeProperties()) && typeInfo.getTypeProperties().contains(TypeProperty.UNIQUE)) {
        return snowflakeIdGenerator.nextId();
      } else {
        return faker.number().randomNumber();
      }
    } else if (TypeInfos.INT_TYPE_INFO.getTypeClass() == typeInfo.getTypeClass()) {
      return Long.valueOf(faker.number().randomNumber()).intValue();

    } else if (TypeInfos.SHORT_TYPE_INFO.getTypeClass() == typeInfo.getTypeClass()) {
      return Long.valueOf(faker.number().randomNumber()).shortValue();

    } else if (TypeInfos.BYTE_TYPE_INFO.getTypeClass() == typeInfo.getTypeClass()) {
      return Long.valueOf(faker.number().randomNumber()).byteValue();

    } else if (TypeInfos.STRING_TYPE_INFO.getTypeClass() == typeInfo.getTypeClass()) {
      return faker.name().fullName();

    } else if (TypeInfos.BOOLEAN_TYPE_INFO.getTypeClass() == typeInfo.getTypeClass()) {
      return faker.bool().bool();

    } else if (TypeInfos.DOUBLE_TYPE_INFO.getTypeClass() == typeInfo.getTypeClass()) {
      return faker.number().randomDouble(5, lower, upper);

    } else if (TypeInfos.FLOAT_TYPE_INFO.getTypeClass() == typeInfo.getTypeClass()) {
      return Double.valueOf(faker.number().randomDouble(5, lower, upper)).floatValue();

    } else if (TypeInfos.BIG_DECIMAL_TYPE_INFO.getTypeClass() == typeInfo.getTypeClass()) {
      return new BigDecimal(faker.number().randomDouble(5, lower, upper));

    } else if (TypeInfos.BIG_INTEGER_TYPE_INFO.getTypeClass() == typeInfo.getTypeClass()) {
      return new BigInteger(String.valueOf(faker.number().randomNumber()));

    } else if (BasicArrayTypeInfo.BINARY_TYPE_INFO.getTypeClass() == typeInfo.getTypeClass()) {
      return faker.name().fullName().getBytes();

    } else if (TypeInfos.SQL_DATE_TYPE_INFO.getTypeClass() == typeInfo.getTypeClass()) {
      return new java.sql.Date(faker.date().between(fromTimestamp, toTimestamp).getTime());

    } else if (TypeInfos.SQL_TIME_TYPE_INFO.getTypeClass() == typeInfo.getTypeClass()) {
      return new Time(faker.date().between(fromTimestamp, toTimestamp).getTime());

    } else if (TypeInfos.SQL_TIMESTAMP_TYPE_INFO.getTypeClass() == typeInfo.getTypeClass()) {
      return new Timestamp(faker.date().between(fromTimestamp, toTimestamp).getTime());

    } else if (TypeInfos.LOCAL_DATE_TYPE_INFO.getTypeClass() == typeInfo.getTypeClass()) {
      return faker.date().between(fromTimestamp, toTimestamp).toLocalDateTime().toLocalDate();

    } else if (TypeInfos.LOCAL_TIME_TYPE_INFO.getTypeClass() == typeInfo.getTypeClass()) {
      return faker.date().between(fromTimestamp, toTimestamp).toLocalDateTime().toLocalTime();

    } else if (TypeInfos.LOCAL_DATE_TIME_TYPE_INFO.getTypeClass() == typeInfo.getTypeClass()) {
      return faker.date().between(fromTimestamp, toTimestamp).toLocalDateTime();

    } else if (TypeInfos.VOID_TYPE_INFO.getTypeClass() == typeInfo.getTypeClass()) {
      return null;
    }

    if (typeInfo instanceof ListTypeInfo) {
      ListTypeInfo<?> listTypeInfo = (ListTypeInfo<?>) typeInfo;
      return Lists.newArrayList(fakeRawValue(listTypeInfo.getElementTypeInfo()));
    }

    if (typeInfo instanceof MapTypeInfo) {
      MapTypeInfo<?, ?> mapTypeInfo = (MapTypeInfo<?, ?>) typeInfo;
      Map<Object, Object> mapRawValue = Maps.newHashMap();
      mapRawValue.put(fakeRawValue(mapTypeInfo.getKeyTypeInfo()), fakeRawValue(mapTypeInfo.getValueTypeInfo()));
      return mapRawValue;
    }
    throw new RuntimeException("Unsupported type " + typeInfo);
  }
}
