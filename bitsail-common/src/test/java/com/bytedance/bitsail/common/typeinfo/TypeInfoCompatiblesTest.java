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

import com.google.common.collect.Sets;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.MathContext;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

public class TypeInfoCompatiblesTest {
  private static final Logger LOG = LoggerFactory.getLogger(TypeInfoCompatiblesTest.class);

  private TypeInfoCompatibles typeInfoCompatibles;

  @Before
  public void before() {
    typeInfoCompatibles = new TypeInfoCompatibles(BitSailConfiguration.newDefault());
  }

  @Test
  public void testIntTypeInfoCompatibles() {
    int value = 100;
    TypeInfo<Integer> source = TypeInfos.INT_TYPE_INFO;
    Assert.assertEquals(assertTypeInfo(source, value), 8);
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
    Assert.assertEquals(assertTypeInfo(source, value), 9);
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
    Assert.assertEquals(assertTypeInfo(source, value), 5);

    value = new BigDecimal("23232323211.11111111111", MathContext.DECIMAL32);
    Object result;
    TypeInfo<?> target;
    target = TypeInfos.STRING_TYPE_INFO;
    result = typeInfoCompatibles.compatibleTo(source, target, value);
    Assert.assertTrue(StringUtils.containsNone((String) result, "E"));
  }

  @Test
  public void testDoubleTypeInfoCompatibles() {
    double value = 1.231d;
    TypeInfo<?> source = TypeInfos.DOUBLE_TYPE_INFO;

    Assert.assertEquals(assertTypeInfo(source, value), 5);

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

    value = Double.parseDouble(String.valueOf(Float.MAX_VALUE)) * 10d;
    target = TypeInfos.FLOAT_TYPE_INFO;
    result = typeInfoCompatibles.compatibleTo(source, target, value);
    assertTypeInfo(result, target);
  }

  @Test
  public void testFloatTypeInfoCompatibles() {
    float value = 1.231f;
    TypeInfo<?> source = TypeInfos.FLOAT_TYPE_INFO;

    Assert.assertEquals(assertTypeInfo(source, value), 5);

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

    Assert.assertEquals(assertTypeInfo(source, numberStr, Sets.newHashSet(TypeInfos.LOCAL_DATE_TIME_TYPE_INFO,
        TypeInfos.LOCAL_DATE_TYPE_INFO,
        TypeInfos.LOCAL_TIME_TYPE_INFO,
        TypeInfos.SQL_DATE_TYPE_INFO,
        TypeInfos.SQL_TIME_TYPE_INFO,
        TypeInfos.SQL_TIMESTAMP_TYPE_INFO)), 15);

    Object result;
    TypeInfo<?> target;

    String booleanStr = "true";
    target = TypeInfos.BOOLEAN_TYPE_INFO;
    result = typeInfoCompatibles.compatibleTo(source, target, booleanStr);
    assertTypeInfo(result, target);

    String timestampStr = "2021-01-01";
    target = TypeInfos.LOCAL_DATE_TIME_TYPE_INFO;
    result = typeInfoCompatibles.compatibleTo(source, target, timestampStr);
    assertTypeInfo(result, target);

    timestampStr = "2021-01-01";
    target = TypeInfos.SQL_TIMESTAMP_TYPE_INFO;
    result = typeInfoCompatibles.compatibleTo(source, target, timestampStr);
    assertTypeInfo(result, target);

    timestampStr = "2021-01-01 10:01:23";
    result = typeInfoCompatibles.compatibleTo(source, target, timestampStr);
    assertTypeInfo(result, target);

    timestampStr = "2021-01-01 10:01:23.1";
    result = typeInfoCompatibles.compatibleTo(source, target, timestampStr);
    assertTypeInfo(result, target);

    timestampStr = "2021-01-01 10:01:23.11";
    result = typeInfoCompatibles.compatibleTo(source, target, timestampStr);
    assertTypeInfo(result, target);

    timestampStr = "2021-01-01 10:01:23.111";
    result = typeInfoCompatibles.compatibleTo(source, target, timestampStr);
    assertTypeInfo(result, target);

    timestampStr = "2021-01-01 10:01:23.1111";
    result = typeInfoCompatibles.compatibleTo(source, target, timestampStr);
    assertTypeInfo(result, target);

    timestampStr = "2021-01-01 10:01:23.111111";
    result = typeInfoCompatibles.compatibleTo(source, target, timestampStr);
    assertTypeInfo(result, target);

    target = TypeInfos.SQL_DATE_TYPE_INFO;
    result = typeInfoCompatibles.compatibleTo(source, target, timestampStr);
    assertTypeInfo(result, target);

    target = TypeInfos.SQL_TIME_TYPE_INFO;
    result = typeInfoCompatibles.compatibleTo(source, target, timestampStr);
    assertTypeInfo(result, target);

    target = TypeInfos.LOCAL_DATE_TIME_TYPE_INFO;
    result = typeInfoCompatibles.compatibleTo(source, target, timestampStr);
    assertTypeInfo(result, target);

    target = TypeInfos.LOCAL_DATE_TYPE_INFO;
    result = typeInfoCompatibles.compatibleTo(source, target, timestampStr);
    assertTypeInfo(result, target);

    target = TypeInfos.LOCAL_TIME_TYPE_INFO;
    result = typeInfoCompatibles.compatibleTo(source, target, timestampStr);
    assertTypeInfo(result, target);
  }

  @Test
  public void testSqlDateTypeInfoCompatibles() {
    Date date = new Date(System.currentTimeMillis());
    TypeInfo<?> source = TypeInfos.SQL_DATE_TYPE_INFO;

    Assert.assertEquals(assertTypeInfo(source, date), 4);
  }

  @Test
  public void testSqlTimeTypeInfoCompatibles() {
    Time time = new Time(System.currentTimeMillis());
    TypeInfo<?> source = TypeInfos.SQL_TIME_TYPE_INFO;

    Assert.assertEquals(assertTypeInfo(source, time), 3);
  }

  @Test
  public void testSqlTimestampTypeInfoCompatibles() {
    long timestamp = System.currentTimeMillis();
    TypeInfo<?> source = TypeInfos.SQL_TIMESTAMP_TYPE_INFO;

    Assert.assertEquals(assertTypeInfo(source, new Timestamp(timestamp)), 7);

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

    Assert.assertEquals(assertTypeInfo(source, localDateTime), 7);
  }

  @Test
  public void testLocalTimeTypeInfoCompatibles() {
    LocalTime localTime = LocalTime.now();
    TypeInfo<?> source = TypeInfos.LOCAL_TIME_TYPE_INFO;

    Assert.assertEquals(assertTypeInfo(source, localTime), 2);
  }

  @Test
  public void testLocalDateTypeInfoCompatibles() {
    LocalDate localDate = LocalDate.now();
    TypeInfo<?> source = TypeInfos.LOCAL_DATE_TYPE_INFO;

    Assert.assertEquals(assertTypeInfo(source, localDate), 2);
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

  private int assertTypeInfo(TypeInfo<?> source, Object value) {
    return assertTypeInfo(source, value, Sets.newHashSet());
  }

  private int assertTypeInfo(TypeInfo<?> source, Object value, Set<TypeInfo<?>> excluded) {
    Map<TypeInfo<?>, Function<Object, Object>> targets = typeInfoCompatibles.getCompatibles()
        .row(source);

    if (MapUtils.isEmpty(targets)) {
      return 0;
    }

    for (Map.Entry<TypeInfo<?>, Function<Object, Object>> entry : targets.entrySet()) {
      if (excluded.contains(entry.getKey())) {
        continue;
      }
      Object result = typeInfoCompatibles.compatibleTo(source, entry.getKey(), value);
      LOG.info("Converted type info from {} to {} for value {}, result {}.", source, entry.getKey(), value, result);
      assertTypeInfo(result, entry.getKey());
    }
    return targets.keySet().size();
  }

  private static void assertTypeInfo(Object value, TypeInfo<?> typeInfo) {
    Assert.assertTrue(value.getClass().isAssignableFrom(typeInfo.getTypeClass()));
  }
}