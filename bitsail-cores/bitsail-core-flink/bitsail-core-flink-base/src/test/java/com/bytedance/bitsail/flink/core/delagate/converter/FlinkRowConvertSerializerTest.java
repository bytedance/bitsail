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

package com.bytedance.bitsail.flink.core.delagate.converter;

import com.bytedance.bitsail.common.column.DoubleColumn;
import com.bytedance.bitsail.common.column.ListColumn;
import com.bytedance.bitsail.common.column.LongColumn;
import com.bytedance.bitsail.common.column.MapColumn;
import com.bytedance.bitsail.common.column.StringColumn;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.model.ColumnInfo;
import com.bytedance.bitsail.common.type.BitSailTypeInfoConverter;
import com.bytedance.bitsail.common.type.TypeInfoConverter;
import com.bytedance.bitsail.common.typeinfo.RowTypeInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfoUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import junit.framework.Assert;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class FlinkRowConvertSerializerTest {
  private FlinkRowConverter flinkRowConvertSerializer;

  @Before
  public void init() {
    List<ColumnInfo> columns = ImmutableList.of(
        new ColumnInfo("col1", "string"),
        new ColumnInfo("col2", "int"),
        new ColumnInfo("col3", "double"),
        new ColumnInfo("col4", "list<string>"),
        new ColumnInfo("col5", "map<string,int>")
    );
    TypeInfoConverter converter = new BitSailTypeInfoConverter();
    BitSailConfiguration conf = BitSailConfiguration.newDefault();
    RowTypeInfo rowTypeInfo = TypeInfoUtils.getRowTypeInfo(converter, columns);
    flinkRowConvertSerializer = new FlinkRowConverter(rowTypeInfo, conf);
  }

  @Test
  public void serializeTest() throws IOException {
    com.bytedance.bitsail.common.row.Row row = new com.bytedance.bitsail.common.row.Row(new Object[] {"test"});
    Row serialize = flinkRowConvertSerializer.to(row);
    com.bytedance.bitsail.common.row.Row deserialize = flinkRowConvertSerializer.from(serialize);
    Assert.assertEquals(row.getField(0), deserialize.getField(0));
  }

  @Test
  public void deserializeTest() throws IOException {
    String stringValue = "test";
    Integer intValue = 1;
    Double doubleValue = 3.14;
    List<String> listValue = ImmutableList.of("list1", "list2");
    Map<String, Integer> mapValue = ImmutableMap.of("key1", 1, "key2", 2);
    Row row = Row.of(
        stringValue,
        intValue,
        doubleValue,
        listValue,
        mapValue
    );
    com.bytedance.bitsail.common.row.Row bitSailRow = flinkRowConvertSerializer.from(row);
    assertEquals(bitSailRow.getFields().length, 5);
    assertEquals(bitSailRow.getField(0), stringValue);
    assertEquals(bitSailRow.getField(1), intValue);
    assertEquals(bitSailRow.getField(2), doubleValue);
    assertEquals(bitSailRow.getField(3), listValue);
    assertEquals(bitSailRow.getField(4), mapValue);
  }

  @Test
  public void bitSailColumnDeserializeTest() throws IOException {
    String stringValue = "test";
    Integer intValue = 1;
    Double doubleValue = 3.14;
    List<StringColumn> listValue = ImmutableList.of(new StringColumn("list1"), new StringColumn(), new StringColumn("list2"));
    Map<StringColumn, LongColumn> mapValue = ImmutableMap.of(
        new StringColumn("key1"), new LongColumn(1),
        new StringColumn("key2"), new LongColumn(2),
        new StringColumn("key3"), new LongColumn());
    Row row = Row.of(
        new StringColumn(stringValue),
        new StringColumn(intValue),
        new DoubleColumn(doubleValue),
        new ListColumn<>(listValue, StringColumn.class),
        new MapColumn<>(mapValue, StringColumn.class, LongColumn.class)
    );
    com.bytedance.bitsail.common.row.Row bitSailRow = flinkRowConvertSerializer.from(row);
    assertEquals(bitSailRow.getFields().length, 5);
    assertEquals(bitSailRow.getField(0), stringValue);
    assertEquals(bitSailRow.getField(1), intValue);
    assertEquals(bitSailRow.getField(2), doubleValue);
    assertEquals(bitSailRow.getField(3), Lists.newArrayList("list1", null, "list2"));
    assertEquals(bitSailRow.getField(4), new HashMap<String, Integer>() {
      {
        put("key1", 1);
        put("key2", 2);
        put("key3", null);
      }
    });
  }
}
