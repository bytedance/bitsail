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

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.util.JsonSerializer;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class TypeInfoValueConverterTest {

  private TypeInfoValueConverter typeInfoValueConverter;

  @Before
  public void before() {
    typeInfoValueConverter = new TypeInfoValueConverter(BitSailConfiguration.newDefault());
  }

  @Test
  public void testMapValue() {
    Map<String, String> maps = Maps.newHashMap();

    maps.put("key1", "value1");
    maps.put("key2", "value2");
    maps.put("key3", "value3");
    Object result;
    result = typeInfoValueConverter.convertObject(maps, TypeInfos.STRING_TYPE_INFO);
    Assert.assertTrue(result instanceof String);
    Map<String, String> resultMap = JsonSerializer.parseToMap((String) result);
    Assert.assertEquals(maps.size(), resultMap.size());

    result = typeInfoValueConverter.convertObject(maps, new MapTypeInfo<>(TypeInfos.STRING_TYPE_INFO, TypeInfos.STRING_TYPE_INFO));
    Assert.assertEquals(maps, result);

    maps.clear();
    maps.put("key1", "1");
    maps.put("key2", "2");
    maps.put("key3", "3");

    result = typeInfoValueConverter.convertObject(maps, new MapTypeInfo<>(TypeInfos.STRING_TYPE_INFO, TypeInfos.LONG_TYPE_INFO));
    Assert.assertNotEquals(maps, result);
    List<?> list = Lists.newArrayList((((Map<?, ?>) result).values()));
    Assert.assertTrue(list.get(0) instanceof Long);
  }

  @Test
  public void testListValue() {
    List<String> list = Lists.newArrayList();

    list.add("value1");
    list.add("value2");
    list.add("value3");
    Object result;
    result = typeInfoValueConverter.convertObject(list, TypeInfos.STRING_TYPE_INFO);
    Assert.assertTrue(result instanceof String);
    List<String> converted = JsonSerializer.parseToList((String) result, String.class);
    Assert.assertEquals(list.size(), converted.size());

    result = typeInfoValueConverter.convertObject(list, new ListTypeInfo<>(TypeInfos.STRING_TYPE_INFO));
    Assert.assertEquals(list, result);

    list.clear();
    list.add("1");
    list.add("2");
    list.add("3");

    result = typeInfoValueConverter.convertObject(list, new ListTypeInfo<>(TypeInfos.LONG_TYPE_INFO));
    Assert.assertNotEquals(list, result);
    Assert.assertTrue(((List<?>) result).get(0) instanceof Long);
  }

  @Test
  public void testPrimitive() {
    String key = new String("KEY");
    Object result;
    result = typeInfoValueConverter.convertObject(key, TypeInfos.STRING_TYPE_INFO);
    Assert.assertEquals(result, key);
  }
}