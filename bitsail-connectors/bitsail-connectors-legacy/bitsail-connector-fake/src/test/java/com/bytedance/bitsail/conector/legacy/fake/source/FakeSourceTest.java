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

package com.bytedance.bitsail.conector.legacy.fake.source;

import com.bytedance.bitsail.common.model.ColumnInfo;
import com.bytedance.bitsail.common.type.BitSailTypeInfoConverter;
import com.bytedance.bitsail.common.util.Preconditions;
import com.bytedance.bitsail.connector.legacy.fake.source.FakeSource;
import com.bytedance.bitsail.flink.core.typeinfo.PrimitiveColumnTypeInfo;
import com.bytedance.bitsail.flink.core.typeutils.ColumnFlinkTypeInfoUtil;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class FakeSourceTest {

  @Test
  public void testEmptyUniqueFieldsMapping() {
    Assert.assertTrue(FakeSource.initUniqueFieldsMapping(null).isEmpty());
    Assert.assertTrue(FakeSource.initUniqueFieldsMapping("   ").isEmpty());
  }

  @Test
  public void testUniqueFieldsMapping() {
    Map<String, Set<String>> fieldMapping = FakeSource.initUniqueFieldsMapping("id,date");
    Assert.assertEquals(2, fieldMapping.size());
    Assert.assertTrue(fieldMapping.containsKey("id"));
    Assert.assertTrue(fieldMapping.containsKey("date"));
  }

  @Test
  public void testConstructRandomValueWithoutUniqueCheck() {
    long expectValue = 1234L;
    long actualValue = FakeSource.constructRandomValue(null, () -> expectValue);
    Assert.assertEquals(expectValue, actualValue);
  }

  @Test
  public void testConstructRandomValueWithUniqueCheck() {
    AtomicInteger constructCount = new AtomicInteger(0);
    Set<String> existValues = new HashSet<>();
    existValues.add("1234");

    long actualValue = FakeSource.constructRandomValue(existValues,
        () -> (constructCount.getAndIncrement() == 0) ? 1234L : 5678);

    Assert.assertEquals(5678, actualValue);
    Assert.assertTrue(existValues.contains("5678"));
  }

  @Test
  public void testSupportByteType() {
    List<ColumnInfo> columnInfos = new ArrayList<>();
    ColumnInfo age = new ColumnInfo("age", "byte");
    columnInfos.add(age);
//    FakeSource fs = new FakeSource()
    RowTypeInfo rowTypeInfo = ColumnFlinkTypeInfoUtil.getRowTypeInformation(new BitSailTypeInfoConverter(), columnInfos);
    Assert.assertTrue(rowTypeInfo.getFieldTypes()[0] instanceof PrimitiveColumnTypeInfo);
  }
}
