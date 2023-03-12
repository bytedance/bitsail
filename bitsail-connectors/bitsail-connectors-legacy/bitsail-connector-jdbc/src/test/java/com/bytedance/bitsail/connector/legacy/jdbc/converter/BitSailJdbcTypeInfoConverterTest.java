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

package com.bytedance.bitsail.connector.legacy.jdbc.converter;

import com.bytedance.bitsail.common.typeinfo.TypeInfo;
import com.bytedance.bitsail.common.util.TypeConvertUtil.StorageEngine;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

public class BitSailJdbcTypeInfoConverterTest {
  private JdbcTypeInfoConverter converter;

  @Before
  public void before() {
    this.converter = new JdbcTypeInfoConverter(StorageEngine.mysql.name());
  }

  @Test
  public void testBitType() {
    TypeInfo<?> typeInfo = converter.fromTypeString("bit");
    Assert.assertEquals(typeInfo.getTypeClass(), Short.class);
  }

  @Test
  public void testEnumType() {
    TypeInfo<?> typeInfo = converter.fromTypeString("enum");
    Assert.assertEquals(typeInfo.getTypeClass(), String.class);
  }

  @Test
  public void testTimeStampType() {
    TypeInfo<?> typeInfo = converter.fromTypeString("timestamp");
    Assert.assertEquals(typeInfo.getTypeClass(), Timestamp.class);
  }

  @Test
  public void testDateType() {
    TypeInfo<?> typeInfo = converter.fromTypeString("date");
    Assert.assertEquals(typeInfo.getTypeClass(), Date.class);
  }

  @Test
  public void testTimeType() {
    TypeInfo<?> typeInfo = converter.fromTypeString("time");
    Assert.assertEquals(typeInfo.getTypeClass(), Time.class);
  }

  @Test
  public void testNumberType() {
    TypeInfo<?> tinyIntTypeInfo = converter.fromTypeString("tinyint");
    Assert.assertEquals(tinyIntTypeInfo.getTypeClass(), Short.class);

    TypeInfo<?> tinyintUnsignedTypeInfo = converter.fromTypeString("tinyint unsigned");
    Assert.assertEquals(tinyintUnsignedTypeInfo.getTypeClass(), Short.class);

    TypeInfo<?> smallintTypeInfo = converter.fromTypeString("smallint");
    Assert.assertEquals(smallintTypeInfo.getTypeClass(), Long.class);

    TypeInfo<?> smallintUnsignedTypeInfo = converter.fromTypeString("smallint unsigned");
    Assert.assertEquals(smallintUnsignedTypeInfo.getTypeClass(), Long.class);

    TypeInfo<?> mediumintTypeInfo = converter.fromTypeString("mediumint");
    Assert.assertEquals(mediumintTypeInfo.getTypeClass(), Long.class);

    TypeInfo<?> mediumintUnsignedTypeInfo = converter.fromTypeString("mediumint unsigned");
    Assert.assertEquals(mediumintUnsignedTypeInfo.getTypeClass(), Long.class);

    TypeInfo<?> intTypeInfo = converter.fromTypeString("int");
    Assert.assertEquals(intTypeInfo.getTypeClass(), Long.class);

    TypeInfo<?> intUnsignedTypeInfo = converter.fromTypeString("int unsigned");
    Assert.assertEquals(intUnsignedTypeInfo.getTypeClass(), Long.class);

    TypeInfo<?> bigintTypeInfo = converter.fromTypeString("bigint");
    Assert.assertEquals(bigintTypeInfo.getTypeClass(), Long.class);

    TypeInfo<?> bigintUnsignedTypeInfo = converter.fromTypeString("bigint unsigned");
    Assert.assertEquals(bigintUnsignedTypeInfo.getTypeClass(), Long.class);

    TypeInfo<?> floatTypeInfo = converter.fromTypeString("float");
    Assert.assertEquals(floatTypeInfo.getTypeClass(), Double.class);

    TypeInfo<?> floatUnsignedTypeInfo = converter.fromTypeString("float unsigned");
    Assert.assertEquals(floatUnsignedTypeInfo.getTypeClass(), Double.class);

    TypeInfo<?> doubleTypeInfo = converter.fromTypeString("double");
    Assert.assertEquals(doubleTypeInfo.getTypeClass(), Double.class);

    TypeInfo<?> doubleUnsignedTypeInfo = converter.fromTypeString("double unsigned");
    Assert.assertEquals(doubleUnsignedTypeInfo.getTypeClass(), Double.class);

    TypeInfo<?> bigDecimalTypeInfo = converter.fromTypeString("decimal");
    Assert.assertEquals(bigDecimalTypeInfo.getTypeClass(), BigDecimal.class);

    TypeInfo<?> bigDecimalUnsignedTypeInfo = converter.fromTypeString("decimal");
    Assert.assertEquals(bigDecimalUnsignedTypeInfo.getTypeClass(), BigDecimal.class);

    TypeInfo<?> realTypeInfo = converter.fromTypeString("real");
    Assert.assertEquals(realTypeInfo.getTypeClass(), Double.class);
  }

  @Test
  public void testYearType() {
    TypeInfo<?> typeInfo = converter.fromTypeString("year");
    Assert.assertEquals(typeInfo.getTypeClass(), Integer.class);
  }

  @Test
  public void testCharType() {
    TypeInfo<?> typeInfo = converter.fromTypeString("char");
    Assert.assertEquals(typeInfo.getTypeClass(), String.class);
  }

  @Test
  public void testVarCharType() {
    TypeInfo<?> typeInfo = converter.fromTypeString("varchar");
    Assert.assertEquals(typeInfo.getTypeClass(), String.class);
  }

  @Test
  public void testLongVarChar() {
    TypeInfo<?> typeInfo = converter.fromTypeString("longvarchar");
    Assert.assertEquals(typeInfo.getTypeClass(), String.class);
  }

}
