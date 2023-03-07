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

package com.bytedance.bitsail.connector.legacy.mongodb.source;

import com.bytedance.bitsail.common.type.filemapping.FileMappingTypeInfoConverter;
import com.bytedance.bitsail.common.typeinfo.TypeInfo;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Timestamp;

public class MongoTypeInfoConverterTest {
  private FileMappingTypeInfoConverter converter;

  @Before
  public void before() {
    this.converter = new FileMappingTypeInfoConverter("mongodb");
  }

  @Test
  public void testTimeStampType() {
    TypeInfo<?> typeInfo = converter.fromTypeString("timestamp");
    Assert.assertEquals(typeInfo.getTypeClass(), Timestamp.class);
  }

  @Test
  public void testDateType() {
    TypeInfo<?> typeInfo = converter.fromTypeString("date");
    Assert.assertEquals(typeInfo.getTypeClass(), Timestamp.class);
  }

  @Test
  public void testNumberType() {
    TypeInfo<?> intTypeInfo = converter.fromTypeString("int");
    Assert.assertEquals(intTypeInfo.getTypeClass(), Integer.class);

    TypeInfo<?> doubleTypeInfo = converter.fromTypeString("double");
    Assert.assertEquals(doubleTypeInfo.getTypeClass(), Double.class);

    TypeInfo<?> bigDecimalTypeInfo = converter.fromTypeString("decimal");
    Assert.assertEquals(bigDecimalTypeInfo.getTypeClass(), BigDecimal.class);

    TypeInfo<?> bigDecimalUnsignedTypeInfo = converter.fromTypeString("decimal");
    Assert.assertEquals(bigDecimalUnsignedTypeInfo.getTypeClass(), BigDecimal.class);
  }

  @Test
  public void testStringType() {
    TypeInfo<?> objectTypeInfo = converter.fromTypeString("objectid");
    Assert.assertEquals(objectTypeInfo.getTypeClass(), String.class);

    TypeInfo<?> stringTypeInfo = converter.fromTypeString("string");
    Assert.assertEquals(stringTypeInfo.getTypeClass(), String.class);

    TypeInfo<?> regexTypeInfo = converter.fromTypeString("regex");
    Assert.assertEquals(regexTypeInfo.getTypeClass(), String.class);

    TypeInfo<?> javascriptTypeInfo = converter.fromTypeString("javascript");
    Assert.assertEquals(javascriptTypeInfo.getTypeClass(), String.class);

    TypeInfo<?> javascriptWithScopeTypeInfo = converter.fromTypeString("javascriptwithscope");
    Assert.assertEquals(javascriptWithScopeTypeInfo.getTypeClass(), String.class);

    TypeInfo<?> undefinedTypeInfo = converter.fromTypeString("undefined");
    Assert.assertEquals(undefinedTypeInfo.getTypeClass(), String.class);
  }

  @Test
  public void testBoolType() {
    TypeInfo<?> typeInfo = converter.fromTypeString("bool");
    Assert.assertEquals(typeInfo.getTypeClass(), Boolean.class);
  }

  @Test
  public void testNullType() {
    TypeInfo<?> typeInfo = converter.fromTypeString("null");
    Assert.assertEquals(typeInfo.getTypeClass(), Void.class);
  }

}
