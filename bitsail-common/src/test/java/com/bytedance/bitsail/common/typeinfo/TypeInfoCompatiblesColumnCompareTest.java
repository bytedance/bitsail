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

import com.bytedance.bitsail.common.column.ColumnCast;
import com.bytedance.bitsail.common.column.DateColumn;
import com.bytedance.bitsail.common.column.DoubleColumn;
import com.bytedance.bitsail.common.column.LongColumn;
import com.bytedance.bitsail.common.column.StringColumn;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.concurrent.TimeUnit;

public class TypeInfoCompatiblesColumnCompareTest {

  private TypeInfoCompatibles typeInfoCompatibles;

  @Before
  public void before() {
    typeInfoCompatibles = new TypeInfoCompatibles(BitSailConfiguration.newDefault());
    ColumnCast.refresh();
    ColumnCast.initColumnCast(BitSailConfiguration.newDefault());
  }

  @Test
  public void testLongColumn() {
    long value = Long.MAX_VALUE;
    LongColumn longColumn = new LongColumn(value);
    TypeInfo<?> source = TypeInfos.LONG_TYPE_INFO;
    Assert.assertEquals(longColumn.asLong(),
        typeInfoCompatibles.compatibleTo(source, TypeInfos.LONG_TYPE_INFO, value));
    Assert.assertEquals(longColumn.asBigDecimal(),
        typeInfoCompatibles.compatibleTo(source, TypeInfos.BIG_DECIMAL_TYPE_INFO, value));
    Assert.assertEquals(longColumn.asBigInteger(),
        typeInfoCompatibles.compatibleTo(source, TypeInfos.BIG_INTEGER_TYPE_INFO, value));
    Assert.assertEquals(longColumn.asBoolean(),
        typeInfoCompatibles.compatibleTo(source, TypeInfos.BOOLEAN_TYPE_INFO, value));
    Assert.assertEquals(longColumn.asString(),
        typeInfoCompatibles.compatibleTo(source, TypeInfos.STRING_TYPE_INFO, value));

    value = System.currentTimeMillis();
    longColumn = new LongColumn(value / 1000);

    Assert.assertEquals(TimeUnit.MILLISECONDS.toSeconds(longColumn.asDate().getTime()),
        TimeUnit.MILLISECONDS.toSeconds(((Timestamp) typeInfoCompatibles.compatibleTo(source, TypeInfos.SQL_TIMESTAMP_TYPE_INFO, value)).getTime()));
  }

  @Test
  public void testDoubleColumn() {
    Double value = 1.23223212323D;
    DoubleColumn doubleColumn = new DoubleColumn(value);
    TypeInfo<?> source = TypeInfos.DOUBLE_TYPE_INFO;
    Assert.assertEquals(doubleColumn.asLong(),
        typeInfoCompatibles.compatibleTo(source, TypeInfos.LONG_TYPE_INFO, value));
    Assert.assertEquals(doubleColumn.asBigDecimal(),
        typeInfoCompatibles.compatibleTo(source, TypeInfos.BIG_DECIMAL_TYPE_INFO, value));
    Assert.assertEquals(doubleColumn.asBigInteger(),
        typeInfoCompatibles.compatibleTo(source, TypeInfos.BIG_INTEGER_TYPE_INFO, value));
    Assert.assertEquals(doubleColumn.asString(),
        typeInfoCompatibles.compatibleTo(source, TypeInfos.STRING_TYPE_INFO, value));
    Assert.assertEquals(doubleColumn.asDouble(),
        typeInfoCompatibles.compatibleTo(source, TypeInfos.DOUBLE_TYPE_INFO, value));

    Float valueFloat = 1.23223212323f;
    doubleColumn = new DoubleColumn(valueFloat);
    source = TypeInfos.FLOAT_TYPE_INFO;
    Assert.assertEquals(doubleColumn.asLong(),
        typeInfoCompatibles.compatibleTo(source, TypeInfos.LONG_TYPE_INFO, valueFloat));
    Assert.assertEquals(doubleColumn.asBigDecimal(),
        typeInfoCompatibles.compatibleTo(source, TypeInfos.BIG_DECIMAL_TYPE_INFO, valueFloat));
    Assert.assertEquals(doubleColumn.asBigInteger(),
        typeInfoCompatibles.compatibleTo(source, TypeInfos.BIG_INTEGER_TYPE_INFO, valueFloat));
    Assert.assertEquals(doubleColumn.asString(),
        typeInfoCompatibles.compatibleTo(source, TypeInfos.STRING_TYPE_INFO, valueFloat));
    Assert.assertEquals(doubleColumn.asDouble(),
        typeInfoCompatibles.compatibleTo(source, TypeInfos.DOUBLE_TYPE_INFO, valueFloat));

    Long valueLong = Long.MAX_VALUE;
    doubleColumn = new DoubleColumn(valueLong);
    source = TypeInfos.LONG_TYPE_INFO;
    Assert.assertEquals(doubleColumn.asLong(),
        typeInfoCompatibles.compatibleTo(source, TypeInfos.LONG_TYPE_INFO, valueLong));
    Assert.assertEquals(doubleColumn.asBigDecimal(),
        typeInfoCompatibles.compatibleTo(source, TypeInfos.BIG_DECIMAL_TYPE_INFO, valueLong));
    Assert.assertEquals(doubleColumn.asBigInteger(),
        typeInfoCompatibles.compatibleTo(source, TypeInfos.BIG_INTEGER_TYPE_INFO, valueLong));
    Assert.assertEquals(doubleColumn.asString(),
        typeInfoCompatibles.compatibleTo(source, TypeInfos.STRING_TYPE_INFO, valueLong));
    Assert.assertEquals(doubleColumn.asDouble(),
        typeInfoCompatibles.compatibleTo(source, TypeInfos.DOUBLE_TYPE_INFO, valueLong));

    Integer valueInteger = Integer.MAX_VALUE;
    doubleColumn = new DoubleColumn(valueInteger);
    source = TypeInfos.INT_TYPE_INFO;
    Assert.assertEquals(doubleColumn.asLong(),
        typeInfoCompatibles.compatibleTo(source, TypeInfos.LONG_TYPE_INFO, valueInteger));
    Assert.assertEquals(doubleColumn.asBigDecimal(),
        typeInfoCompatibles.compatibleTo(source, TypeInfos.BIG_DECIMAL_TYPE_INFO, valueInteger));
    Assert.assertEquals(doubleColumn.asBigInteger(),
        typeInfoCompatibles.compatibleTo(source, TypeInfos.BIG_INTEGER_TYPE_INFO, valueInteger));
    Assert.assertEquals(doubleColumn.asString(),
        typeInfoCompatibles.compatibleTo(source, TypeInfos.STRING_TYPE_INFO, valueInteger));
    Assert.assertEquals(doubleColumn.asDouble(),
        typeInfoCompatibles.compatibleTo(source, TypeInfos.DOUBLE_TYPE_INFO, valueInteger));

    BigDecimal valueBigDecimal = BigDecimal.valueOf(1.23232123d);
    doubleColumn = new DoubleColumn(valueBigDecimal);
    source = TypeInfos.BIG_DECIMAL_TYPE_INFO;
    Assert.assertEquals(doubleColumn.asLong(),
        typeInfoCompatibles.compatibleTo(source, TypeInfos.LONG_TYPE_INFO, valueBigDecimal));
    Assert.assertEquals(doubleColumn.asBigDecimal(),
        typeInfoCompatibles.compatibleTo(source, TypeInfos.BIG_DECIMAL_TYPE_INFO, valueBigDecimal));
    Assert.assertEquals(doubleColumn.asBigInteger(),
        typeInfoCompatibles.compatibleTo(source, TypeInfos.BIG_INTEGER_TYPE_INFO, valueBigDecimal));
    Assert.assertEquals(doubleColumn.asString(),
        typeInfoCompatibles.compatibleTo(source, TypeInfos.STRING_TYPE_INFO, valueBigDecimal));
    Assert.assertEquals(doubleColumn.asDouble(),
        typeInfoCompatibles.compatibleTo(source, TypeInfos.DOUBLE_TYPE_INFO, valueBigDecimal));

    valueBigDecimal = new BigDecimal("23232323211.11111111111", MathContext.DECIMAL32);
    doubleColumn = new DoubleColumn(valueBigDecimal);
    Assert.assertEquals(doubleColumn.asString(),
        typeInfoCompatibles.compatibleTo(source, TypeInfos.STRING_TYPE_INFO, valueBigDecimal));

    BigInteger valueBigInteger = BigInteger.valueOf(Long.MAX_VALUE);
    doubleColumn = new DoubleColumn(valueBigInteger);
    source = TypeInfos.BIG_INTEGER_TYPE_INFO;
    Assert.assertEquals(doubleColumn.asLong(),
        typeInfoCompatibles.compatibleTo(source, TypeInfos.LONG_TYPE_INFO, valueBigInteger));
    Assert.assertEquals(doubleColumn.asBigDecimal(),
        typeInfoCompatibles.compatibleTo(source, TypeInfos.BIG_DECIMAL_TYPE_INFO, valueBigInteger));
    Assert.assertEquals(doubleColumn.asBigInteger(),
        typeInfoCompatibles.compatibleTo(source, TypeInfos.BIG_INTEGER_TYPE_INFO, valueBigInteger));
    Assert.assertEquals(doubleColumn.asString(),
        typeInfoCompatibles.compatibleTo(source, TypeInfos.STRING_TYPE_INFO, valueBigInteger));
  }

  @Test
  public void testStringColumn() {
    String value = "12323.12323";
    StringColumn stringColumn = new StringColumn(value);
    TypeInfo<?> source = TypeInfos.STRING_TYPE_INFO;
    Assert.assertEquals(stringColumn.asBigDecimal(),
        typeInfoCompatibles.compatibleTo(source, TypeInfos.BIG_DECIMAL_TYPE_INFO, value));
    Assert.assertEquals(stringColumn.asString(),
        typeInfoCompatibles.compatibleTo(source, TypeInfos.STRING_TYPE_INFO, value));
    Assert.assertEquals(stringColumn.asDouble(),
        typeInfoCompatibles.compatibleTo(source, TypeInfos.DOUBLE_TYPE_INFO, value));
    Assert.assertArrayEquals((byte[]) stringColumn.asBytes(),
        (byte[]) typeInfoCompatibles.compatibleTo(source, BasicArrayTypeInfo.BINARY_TYPE_INFO, value));

    value = "12323";
    stringColumn = new StringColumn(value);

    Assert.assertEquals(stringColumn.asLong(),
        typeInfoCompatibles.compatibleTo(source, TypeInfos.LONG_TYPE_INFO, value));
    Assert.assertEquals(stringColumn.asLong(),
        typeInfoCompatibles.compatibleTo(source, TypeInfos.LONG_TYPE_INFO, value));
    Assert.assertEquals(stringColumn.asBigInteger(),
        typeInfoCompatibles.compatibleTo(source, TypeInfos.BIG_INTEGER_TYPE_INFO, value));

    value = "2022-10-01";
    stringColumn = new StringColumn(value);
    Assert.assertEquals(stringColumn.asDate(),
        typeInfoCompatibles.compatibleTo(source, TypeInfos.SQL_DATE_TYPE_INFO, value));

    value = "2022-10-01 10:10:10";
    stringColumn = new StringColumn(value);
    Assert.assertEquals(stringColumn.asDate(),
        typeInfoCompatibles.compatibleTo(source, TypeInfos.SQL_TIMESTAMP_TYPE_INFO, value));

    value = "10:10:10";
    stringColumn = new StringColumn(value);
    Assert.assertEquals(stringColumn.asDate(),
        typeInfoCompatibles.compatibleTo(source, TypeInfos.SQL_TIME_TYPE_INFO, value));

    value = "NaN";
    stringColumn = new StringColumn(value);
    Assert.assertEquals(stringColumn.asDouble(),
        typeInfoCompatibles.compatibleTo(source, TypeInfos.DOUBLE_TYPE_INFO, value));

    value = "Infinity";
    stringColumn = new StringColumn(value);
    Assert.assertEquals(stringColumn.asDouble(),
        typeInfoCompatibles.compatibleTo(source, TypeInfos.DOUBLE_TYPE_INFO, value));

    value = "-Infinity";
    stringColumn = new StringColumn(value);
    Assert.assertEquals(stringColumn.asDouble(),
        typeInfoCompatibles.compatibleTo(source, TypeInfos.DOUBLE_TYPE_INFO, value));
  }

  @Test
  public void testDateColumn() {
    Timestamp value = new Timestamp(System.currentTimeMillis());
    DateColumn dateColumn = new DateColumn(value);

    TypeInfo<?> source = TypeInfos.SQL_TIMESTAMP_TYPE_INFO;
    Assert.assertEquals(dateColumn.asString(),
        typeInfoCompatibles.compatibleTo(source, TypeInfos.STRING_TYPE_INFO, value));
    Assert.assertEquals(dateColumn.asLong(),
        typeInfoCompatibles.compatibleTo(source, TypeInfos.LONG_TYPE_INFO, value));
    Assert.assertEquals(dateColumn.asDate().getTime(),
        ((java.util.Date) typeInfoCompatibles.compatibleTo(source, source, value)).getTime());

    Date valueDate = new Date(System.currentTimeMillis());
    dateColumn = new DateColumn(valueDate);
    source = TypeInfos.SQL_DATE_TYPE_INFO;
    Assert.assertEquals(dateColumn.asString(),
        typeInfoCompatibles.compatibleTo(source, TypeInfos.STRING_TYPE_INFO, valueDate));
    Assert.assertEquals(dateColumn.asLong(),
        typeInfoCompatibles.compatibleTo(source, TypeInfos.LONG_TYPE_INFO, valueDate));
    Assert.assertEquals(dateColumn.asDate().getTime(),
        ((java.util.Date) typeInfoCompatibles.compatibleTo(source, source, valueDate)).getTime());

    Time valueTime = new Time(System.currentTimeMillis());
    dateColumn = new DateColumn(valueTime);
    source = TypeInfos.SQL_TIME_TYPE_INFO;
    Assert.assertEquals(dateColumn.asString(),
        typeInfoCompatibles.compatibleTo(source, TypeInfos.STRING_TYPE_INFO, valueTime));
    Assert.assertEquals(dateColumn.asLong(),
        typeInfoCompatibles.compatibleTo(source, TypeInfos.LONG_TYPE_INFO, valueTime));
    Assert.assertEquals(dateColumn.asDate().getTime(),
        ((java.util.Date) typeInfoCompatibles.compatibleTo(source, source, valueTime)).getTime());
  }
}
