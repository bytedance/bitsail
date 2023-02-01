/*
 * Copyright 2022 Bytedance Ltd. and/or its affiliates.
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

package com.bytedance.bitsail.common.type;

import com.bytedance.bitsail.common.type.filemapping.FileMappingTypeInfoConverter;
import com.bytedance.bitsail.common.type.filemapping.FileMappingTypeInfoReader;
import com.bytedance.bitsail.common.typeinfo.TypeInfo;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

/**
 * Created 2022/8/23
 */
public class SimpleTypeInfoConverterTest {

  private FileMappingTypeInfoConverter fileMappingTypeInfoConverter;
  private Map<String, TypeInfo<?>> toTypeInformation;
  private Map<TypeInfo<?>, String> fromTypeInformation;

  @Before
  public void before() {
    fileMappingTypeInfoConverter = new FileMappingTypeInfoConverter("fake");
    FileMappingTypeInfoReader reader = fileMappingTypeInfoConverter.getReader();
    toTypeInformation = reader.getToTypeInformation();
    fromTypeInformation = reader.getFromTypeInformation();
  }

  @Test
  public void testSimpleTypeInfoConverter() {
    Assert.assertEquals(toTypeInformation.size(), 8);
    Assert.assertEquals(fromTypeInformation.size(), 7);
  }

  @Test
  public void testFromTypeString_ListType() {
    TypeInfo<?> typeInfo01 = fileMappingTypeInfoConverter.fromTypeString("Array(double)");
    Assert.assertEquals(typeInfo01.getTypeClass(), List.class);

    TypeInfo<?> typeInfo02 = fileMappingTypeInfoConverter.fromTypeString("Array(Array(double))");
    Assert.assertEquals(typeInfo02.getTypeClass(), List.class);
  }

  @Test
  public void testFromTypeString_MapType() {
    TypeInfo<?> typeInfo01 = fileMappingTypeInfoConverter.fromTypeString("Map(string,double)");
    Assert.assertEquals(typeInfo01.getTypeClass(), Map.class);

    TypeInfo<?> typeInfo02 = fileMappingTypeInfoConverter.fromTypeString("Map(string, Map(string, double))");
    Assert.assertEquals(typeInfo02.getTypeClass(), Map.class);
  }

  @Test
  public void testFromTypeInfo_ListType() {
    TypeInfo<?> typeInfo = fileMappingTypeInfoConverter.fromTypeString("Array(Array(Int32))");
    Assert.assertEquals(typeInfo.getTypeClass(), List.class);

    String typeString = fileMappingTypeInfoConverter.fromTypeInfo(typeInfo);
    Assert.assertEquals(typeString, "Array(Array(Int32))");
  }

  @Test
  public void testFromTypeInfo_MapType() {
    TypeInfo<?> typeInfo = fileMappingTypeInfoConverter.fromTypeString("Map(string, Map(string, double))");

    String typeString = fileMappingTypeInfoConverter.fromTypeInfo(typeInfo);
    Assert.assertEquals(typeString, "Map(string, Map(string, double))");
  }
}