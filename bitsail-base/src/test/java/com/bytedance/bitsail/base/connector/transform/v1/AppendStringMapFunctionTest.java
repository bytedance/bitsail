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

package com.bytedance.bitsail.base.connector.transform.v1;

import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.common.typeinfo.RowTypeInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfos;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class AppendStringMapFunctionTest {
  @Test
  public void testAppendString() throws Exception {
    List<String> indexes = new ArrayList<>();
    indexes.add("c2");
    indexes.add("c3");
    List<String> appendVals = new ArrayList<>();
    appendVals.add("_tail1");
    appendVals.add("_tail2");
    String[] fieldNames = {"c1", "c2", "c3"};
    TypeInfo<?>[] typeInfos = {TypeInfos.INT_TYPE_INFO, TypeInfos.STRING_TYPE_INFO, TypeInfos.STRING_TYPE_INFO};
    RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldNames, typeInfos);
    BitSailMapFunction<Row, Row> mapFunction = new AppendStringMapFunction<>(indexes, appendVals, rowTypeInfo);
    Object[] vals = {1, "field1", "field2"};
    Object[] expected = {1, "field1_tail1", "field2_tail2"};

    // test BitSail row
    Row testRow = new Row(vals.clone());
    Row expectedRow = new Row(expected);
    Row result = mapFunction.map(testRow);
    Assert.assertEquals(expectedRow.getString(1), result.getString(1));
    Assert.assertEquals(expectedRow.getString(2), result.getString(2));
  }
}
