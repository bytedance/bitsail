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

package com.bytedance.bitsail.connector.elasticsearch.source.reader.deserializer;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.common.typeinfo.TypeInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfos;

import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

public class ElasticsearchRowDeserializerTest {

  private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchRowDeserializerTest.class);

  private ElasticsearchRowDeserializer deserializer;

  @Before
  public void setup() {
    BitSailConfiguration jobConf = BitSailConfiguration.newDefault();
    TypeInfo<?>[] typeInfos = new TypeInfo[] {
        TypeInfos.INT_TYPE_INFO,
        TypeInfos.STRING_TYPE_INFO,
        TypeInfos.STRING_TYPE_INFO,
        TypeInfos.LONG_TYPE_INFO,
        TypeInfos.SQL_DATE_TYPE_INFO
    };
    String[] fieldNames = new String[] {
        "id", "text_type", "keyword_type", "long_type", "date_type"
    };
    deserializer = new ElasticsearchRowDeserializer(typeInfos, fieldNames, jobConf);
  }

  @Test
  public void testDeserialize() throws ParseException {
    Date date = new SimpleDateFormat("yyyy-MM-dd").parse("2013-01-17");
    Row testRow = new Row(new Object[]{1, "test1-text-1", "test1-keyword-1", 2023011700L, date});

    Map<String, Object> map = Maps.newHashMap();
    map.put("id", 1);
    map.put("text_type", "test1-text-1");
    map.put("keyword_type", "test1-keyword-1");
    map.put("long_type", 2023011700L);
    map.put("date_type", "2013-01-17");
    Row row = deserializer.deserialize(map);
    Assert.assertEquals(row, testRow);
  }
}
