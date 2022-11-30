/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.bytedance.bitsail.deserializer;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.model.ColumnInfo;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.common.util.FastJsonUtil;
import com.bytedance.bitsail.connector.ftp.option.FtpReaderOptions;
import com.bytedance.bitsail.connector.ftp.source.reader.deserializer.JsonRowDeserializer;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class JsonRowDeserializerTest {
  private static List<ColumnInfo> getColumnInfo(String columnName, String type) {
    List<ColumnInfo> columnInfos = Lists.newArrayListWithExpectedSize(1);
    columnInfos.add(new ColumnInfo(columnName, type));
    return columnInfos;
  }

  @Test
  public void testConvertErrorColumnAsNull() throws IOException {
    BitSailConfiguration jobConf = BitSailConfiguration.newDefault();
    jobConf.set(FtpReaderOptions.CONVERT_ERROR_COLUMN_AS_NULL, true);
    jobConf.set(FtpReaderOptions.COLUMNS, getColumnInfo("c1", "date"));
    JsonRowDeserializer parser = new JsonRowDeserializer(jobConf);
    Row row = parser.convert("{\"c1\":\"aaa\"}");
    Assert.assertTrue(row.getField(0) == null);
  }

  @Test
  public void testWriteMapNullSerializeFeature() throws IOException {
    String line = "{\"a\":\"111\",\"b\":{\"c1\":\"ds\",\"c2\":null}}";
    BitSailConfiguration jobConf = BitSailConfiguration.newDefault();
    jobConf.set(FtpReaderOptions.COLUMNS, getColumnInfo("b", "string"));
    JsonRowDeserializer parser1 = new JsonRowDeserializer(jobConf);
    Row row1 = parser1.convert(line);
    Assert.assertEquals("{\"c1\":\"ds\"}", row1.getField(0).toString());

    jobConf.set(FtpReaderOptions.JSON_SERIALIZER_FEATURES, "WriteMapNullValue");
    JsonRowDeserializer parser2 = new JsonRowDeserializer(jobConf);
    Row row2 = parser2.convert(line);
    Assert.assertEquals("{\"c1\":\"ds\",\"c2\":null}", row2.getField(0).toString());
  }

  @Test
  public void testConvertJsonKeyToLowerCase() {
    BitSailConfiguration jobConf = BitSailConfiguration.newDefault();
    jobConf.set(FtpReaderOptions.COLUMNS, getColumnInfo("b", "string"));
    JsonRowDeserializer jsonRowDeserializer = new JsonRowDeserializer(jobConf);

    String s = "{\"A\": [{\"E\": [{\"A\": 3, \"B\": 4}, {\"C\": 5, \"D\": 6}]}]}";
    String result = "{\"a\":[{\"e\":[{\"a\":3,\"b\":4},{\"c\":5,\"d\":6}]}]}";
    JSONObject jo = (JSONObject) jsonRowDeserializer.convertJsonKeyToLowerCase(FastJsonUtil.parseObject(s));
    assertEquals(result, jo.toString());
  }
}
