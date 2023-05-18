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
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;

import org.apache.commons.collections.MapUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Map;
import java.util.function.Function;

public class TypeInfoCompatiblesTest {

  private TypeInfoCompatibles typeInfoCompatibles;

  @Before
  public void before() {
    typeInfoCompatibles = new TypeInfoCompatibles(BitSailConfiguration.newDefault());
  }

  @Test
  public void testIntTypeInfoCompatibles() {
    int value = 100;
    TypeInfo<Integer> source = TypeInfos.INT_TYPE_INFO;
    assertTypeInfo(source, value);
  }

  @Test
  public void testIntTypeInfoCompatiblesOverflow() {
    int value = Integer.MAX_VALUE;
    TypeInfo<?> source = TypeInfos.LONG_TYPE_INFO;

    Assert.assertThrows(BitSailException.class,
        () -> typeInfoCompatibles.compatibleTo(source, TypeInfos.SHORT_TYPE_INFO, value));
  }

  @Test
  public void testLongTypeInfoCompatibles() {
    long value = 1000L;
    TypeInfo<?> source = TypeInfos.LONG_TYPE_INFO;
    assertTypeInfo(source, value);
  }

  @Test
  public void testLongTypeInfoCompatiblesOverflow() {
    long value = Long.MAX_VALUE;
    TypeInfo<?> source = TypeInfos.LONG_TYPE_INFO;

    Assert.assertThrows(BitSailException.class,
        () -> typeInfoCompatibles.compatibleTo(source, TypeInfos.SHORT_TYPE_INFO, value));
    Assert.assertThrows(BitSailException.class,
        () -> typeInfoCompatibles.compatibleTo(source, TypeInfos.INT_TYPE_INFO, value));
  }

  @Test
  public void testBigDecimalTypeInfoCompatibles() {
    BigDecimal value = new BigDecimal("87496.4557384283");
    TypeInfo<?> source = TypeInfos.BIG_DECIMAL_TYPE_INFO;
    assertTypeInfo(source, value);
  }

  @Test
  public void testDoubleTypeInfoCompatibles() {
    double value = 1.231d;
    TypeInfo<?> source = TypeInfos.DOUBLE_TYPE_INFO;

    Object result;
    TypeInfo<?> target;
    target = TypeInfos.BIG_DECIMAL_TYPE_INFO;
    result = typeInfoCompatibles.compatibleTo(source, target, value);
    assertTypeInfo(result, target);

    target = TypeInfos.STRING_TYPE_INFO;
    result = typeInfoCompatibles.compatibleTo(source, target, value);
    assertTypeInfo(result, target);

    target = TypeInfos.BIG_INTEGER_TYPE_INFO;
    result = typeInfoCompatibles.compatibleTo(source, target, value);
    assertTypeInfo(result, target);
  }

  @Test
  public void testFloatTypeInfoCompatibles() {
    float value = 1.231f;
    TypeInfo<?> source = TypeInfos.FLOAT_TYPE_INFO;

    Object result;
    TypeInfo<?> target;
    target = TypeInfos.DOUBLE_TYPE_INFO;
    result = typeInfoCompatibles.compatibleTo(source, target, value);
    assertTypeInfo(result, target);

    target = TypeInfos.STRING_TYPE_INFO;
    result = typeInfoCompatibles.compatibleTo(source, target, value);
    assertTypeInfo(result, target);

    target = TypeInfos.BIG_DECIMAL_TYPE_INFO;
    result = typeInfoCompatibles.compatibleTo(source, target, value);
    assertTypeInfo(result, target);
  }

  @Test
  public void testStringTypeInfoCompatibles() {
    String numberStr = "2012";
    TypeInfo<?> source = TypeInfos.STRING_TYPE_INFO;

    Object result;
    TypeInfo<?> target;
    target = TypeInfos.SHORT_TYPE_INFO;
    result = typeInfoCompatibles.compatibleTo(source, target, numberStr);
    assertTypeInfo(result, target);

    target = TypeInfos.INT_TYPE_INFO;
    result = typeInfoCompatibles.compatibleTo(source, target, numberStr);
    assertTypeInfo(result, target);

    target = TypeInfos.LONG_TYPE_INFO;
    result = typeInfoCompatibles.compatibleTo(source, target, numberStr);
    assertTypeInfo(result, target);

    target = BasicArrayTypeInfo.BINARY_TYPE_INFO;
    result = typeInfoCompatibles.compatibleTo(source, target, numberStr);
    assertTypeInfo(result, target);

    target = TypeInfos.BIG_INTEGER_TYPE_INFO;
    result = typeInfoCompatibles.compatibleTo(source, target, numberStr);
    assertTypeInfo(result, target);

    target = TypeInfos.BIG_DECIMAL_TYPE_INFO;
    result = typeInfoCompatibles.compatibleTo(source, target, numberStr);
    assertTypeInfo(result, target);

    String timestampStr = "2021-01-01 10:01:23";
    target = TypeInfos.SQL_TIMESTAMP_TYPE_INFO;
    result = typeInfoCompatibles.compatibleTo(source, target, timestampStr);
    assertTypeInfo(result, target);

    target = TypeInfos.SQL_DATE_TYPE_INFO;
    result = typeInfoCompatibles.compatibleTo(source, target, timestampStr);
    assertTypeInfo(result, target);

    target = TypeInfos.SQL_TIME_TYPE_INFO;
    result = typeInfoCompatibles.compatibleTo(source, target, timestampStr);
    assertTypeInfo(result, target);
  }

  @Test
  public void testSqlDateTypeInfoCompatibles() {
    Date date = new Date(System.currentTimeMillis());
    TypeInfo<?> source = TypeInfos.SQL_DATE_TYPE_INFO;

    Object result;
    TypeInfo<?> target;
    target = TypeInfos.STRING_TYPE_INFO;
    result = typeInfoCompatibles.compatibleTo(source, target, date);
    assertTypeInfo(result, target);

    target = TypeInfos.LOCAL_DATE_TYPE_INFO;
    result = typeInfoCompatibles.compatibleTo(source, target, date);
    assertTypeInfo(result, target);
  }

  @Test
  public void testSqlTimeTypeInfoCompatibles() {
    Time time = new Time(System.currentTimeMillis());
    TypeInfo<?> source = TypeInfos.SQL_TIME_TYPE_INFO;

    Object result;
    TypeInfo<?> target;
    target = TypeInfos.STRING_TYPE_INFO;
    result = typeInfoCompatibles.compatibleTo(source, target, time);
    assertTypeInfo(result, target);

    target = TypeInfos.LOCAL_TIME_TYPE_INFO;
    result = typeInfoCompatibles.compatibleTo(source, target, time);
    assertTypeInfo(result, target);
  }

  @Test
  public void testSqlTimestampTypeInfoCompatibles() {
    long timestamp = System.currentTimeMillis();
    TypeInfo<?> source = TypeInfos.SQL_TIMESTAMP_TYPE_INFO;

    Object result;
    TypeInfo<?> target;
    target = TypeInfos.STRING_TYPE_INFO;
    result = typeInfoCompatibles.compatibleTo(source, target, new Timestamp(timestamp));
    assertTypeInfo(result, target);

    target = TypeInfos.LONG_TYPE_INFO;
    result = typeInfoCompatibles.compatibleTo(source, target, new Timestamp(timestamp));
    assertTypeInfo(result, target);
    Assert.assertEquals((long) result, timestamp);

    target = TypeInfos.LOCAL_DATE_TIME_TYPE_INFO;
    result = typeInfoCompatibles.compatibleTo(source, target, new Timestamp(timestamp));
    assertTypeInfo(result, target);
    Assert.assertEquals(((LocalDateTime) result).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(), timestamp);
  }

  @Test
  public void testLocalDateTimeTypeInfoCompatibles() {
    LocalDateTime localDateTime = LocalDateTime.now();
    TypeInfo<?> source = TypeInfos.LOCAL_DATE_TIME_TYPE_INFO;

    Object result;
    TypeInfo<?> target;
    target = TypeInfos.STRING_TYPE_INFO;
    result = typeInfoCompatibles.compatibleTo(source, target, localDateTime);
    assertTypeInfo(result, target);

    target = TypeInfos.LONG_TYPE_INFO;
    result = typeInfoCompatibles.compatibleTo(source, target, localDateTime);
    assertTypeInfo(result, target);
  }

  @Test
  public void testLocalTimeTypeInfoCompatibles() {
    LocalTime localTime = LocalTime.now();
    TypeInfo<?> source = TypeInfos.LOCAL_TIME_TYPE_INFO;

    Object result;
    TypeInfo<?> target;
    target = TypeInfos.STRING_TYPE_INFO;
    result = typeInfoCompatibles.compatibleTo(source, target, localTime);
    assertTypeInfo(result, target);

    target = TypeInfos.SQL_TIME_TYPE_INFO;
    result = typeInfoCompatibles.compatibleTo(source, target, localTime);
    assertTypeInfo(result, target);
  }

  @Test
  public void testLocalDateTypeInfoCompatibles() {
    LocalDate localDate = LocalDate.now();
    TypeInfo<?> source = TypeInfos.LOCAL_DATE_TYPE_INFO;

    Object result;
    TypeInfo<?> target;
    target = TypeInfos.STRING_TYPE_INFO;
    result = typeInfoCompatibles.compatibleTo(source, target, localDate);
    assertTypeInfo(result, target);

    target = TypeInfos.SQL_DATE_TYPE_INFO;
    result = typeInfoCompatibles.compatibleTo(source, target, localDate);
    assertTypeInfo(result, target);
  }

  @Test
  public void testByteArrayInfoCompatibles() {
    String str = "bit-sail";
    TypeInfo<?> source = TypeInfos.STRING_TYPE_INFO;

    Object result;
    TypeInfo<?> target;
    target = BasicArrayTypeInfo.BINARY_TYPE_INFO;
    result = typeInfoCompatibles.compatibleTo(source, target, str);
    assertTypeInfo(result, target);

    result = typeInfoCompatibles.compatibleTo(target, source, result);
    assertTypeInfo(result, source);

    Assert.assertEquals(str, result);
  }

  private void assertTypeInfo(TypeInfo<?> source, Object value) {
    Map<TypeInfo<?>, Function<Object, Object>> targets = typeInfoCompatibles.getCompatibles()
        .row(source);

    if (MapUtils.isEmpty(targets)) {
      return;
    }

    for (Map.Entry<TypeInfo<?>, Function<Object, Object>> entry : targets.entrySet()) {
      Object result = typeInfoCompatibles.compatibleTo(source, entry.getKey(), value);
      assertTypeInfo(result, entry.getKey());
    }
  }

  private static void assertTypeInfo(Object value, TypeInfo<?> typeInfo) {
    Assert.assertTrue(value.getClass().isAssignableFrom(typeInfo.getTypeClass()));
  }
}