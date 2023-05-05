/*
 * Copyright 2022-2023 Bytedance Ltd. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.bytedance.bitsail.core.flink.bridge.transform.delegate;

import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.common.typeinfo.RowTypeInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfos;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class RowKeySelectorTest {
  @Test
  public void testBitSailRowSelector() throws Exception {
    String[] fieldNames = {"c1", "c2"};
    TypeInfo<?>[] typeInfos = {TypeInfos.INT_TYPE_INFO, TypeInfos.STRING_TYPE_INFO};
    RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldNames, typeInfos);
    Row row = new Row(2);
    row.setField(0, 1);
    row.setField(1, "key");
    assertEquals(1, new RowKeySelector(rowTypeInfo, "c1").getKey(row));
    assertEquals("key", new RowKeySelector(rowTypeInfo, "c2").getKey(row));
  }

  @Test
  public void testFlinkRowSelector() throws Exception {
    String[] fieldNames = {"c1", "c2"};
    TypeInfo<?>[] typeInfos = {TypeInfos.INT_TYPE_INFO, TypeInfos.STRING_TYPE_INFO};
    RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldNames, typeInfos);
    org.apache.flink.types.Row row = new org.apache.flink.types.Row(2);
    row.setField(0, 1);
    row.setField(1, "key");
    assertEquals(1, new RowKeySelector(rowTypeInfo, "c1").getKey(row));
    assertEquals("key", new RowKeySelector(rowTypeInfo, "c2").getKey(row));
  }
}
