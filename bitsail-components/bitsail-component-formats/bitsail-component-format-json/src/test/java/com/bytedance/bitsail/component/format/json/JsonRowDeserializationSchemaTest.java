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

package com.bytedance.bitsail.component.format.json;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.common.typeinfo.RowTypeInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfos;
import com.bytedance.bitsail.component.format.json.JsonRowDeserializationSchema;
import com.bytedance.bitsail.component.format.json.option.JsonReaderOptions;

import org.junit.Assert;
import org.junit.Test;

public class JsonRowDeserializationSchemaTest {
  @Test
  public void testConvertErrorColumnAsNull() {
    BitSailConfiguration jobConf = BitSailConfiguration.newDefault();
    jobConf.set(JsonReaderOptions.CONVERT_ERROR_COLUMN_AS_NULL, true);
    TypeInfo<?>[] typeInfos = {TypeInfos.INT_TYPE_INFO};
    String[] fieldNames = {"c1"};
    String json = "{\"c1\":\"aaa\"}";
    RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldNames, typeInfos);
    JsonRowDeserializationSchema deserializationSchema = new JsonRowDeserializationSchema(jobConf, rowTypeInfo);
    Row row = deserializationSchema.deserialize(json.getBytes());
    Assert.assertNull(row.getField(0));
  }

  @Test
  public void testCaseInsensitive() {
    BitSailConfiguration jobConf = BitSailConfiguration.newDefault();
    jobConf.set(JsonReaderOptions.CASE_INSENSITIVE, true);
    TypeInfo<?>[] typeInfos = {TypeInfos.STRING_TYPE_INFO};
    String[] fieldNames = {"aB1"};
    String json = "{\"Ab1\":\"aaa\"}";
    RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldNames, typeInfos);
    JsonRowDeserializationSchema deserializationSchema = new JsonRowDeserializationSchema(jobConf, rowTypeInfo);
    Row row = deserializationSchema.deserialize(json.getBytes());
    Assert.assertEquals(row.getField(0), "aaa");
  }

}

