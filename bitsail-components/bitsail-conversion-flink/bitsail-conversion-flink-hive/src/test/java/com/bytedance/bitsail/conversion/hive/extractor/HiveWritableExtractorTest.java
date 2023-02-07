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

package com.bytedance.bitsail.conversion.hive.extractor;

import com.bytedance.bitsail.common.column.LongColumn;
import com.bytedance.bitsail.conversion.hive.HiveInspectors;
import com.bytedance.bitsail.conversion.hive.HiveObjectConversion;

import com.bytedance.bitsail.shaded.hive.shim.HiveShimV121;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class HiveWritableExtractorTest {

  @Test
  public void testParseHiveColumnList() {
    String columnNameStr = "col1,col2,col3";
    Assert.assertArrayEquals(new String[] {"col1", "col2", "col3"},
        HiveWritableExtractor.getHiveColumnList(columnNameStr).toArray(new String[0]));
  }

  @Test
  public void testParseHiveTypeInfo() {
    String columnTypeStr = "bigint,string,array<double>,map<int,string>,tinyint";
    List<TypeInfo> typeInfoList = HiveWritableExtractor.getHiveTypeInfos(columnTypeStr);
    Assert.assertEquals("bigint", typeInfoList.get(0).getTypeName());
    Assert.assertEquals("string", typeInfoList.get(1).getTypeName());
    Assert.assertEquals("array<double>", typeInfoList.get(2).getTypeName());
    Assert.assertEquals("map<int,string>", typeInfoList.get(3).getTypeName());
    ObjectInspector objectInspector = HiveInspectors.getObjectInspector(typeInfoList.get(4));
    HiveObjectConversion conversion = HiveInspectors
        .getConversion(objectInspector, typeInfoList.get(4), new HiveShimV121());
    Assert.assertEquals(conversion.toHiveObject(new LongColumn(1L)),
        Long.valueOf(1L).byteValue());
  }
}
