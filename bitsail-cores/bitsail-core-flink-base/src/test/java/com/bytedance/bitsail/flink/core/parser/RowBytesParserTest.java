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

package com.bytedance.bitsail.flink.core.parser;

import com.bytedance.bitsail.common.column.BooleanColumn;
import com.bytedance.bitsail.common.column.BytesColumn;
import com.bytedance.bitsail.common.column.DateColumn;
import com.bytedance.bitsail.common.column.DoubleColumn;
import com.bytedance.bitsail.common.column.ListColumn;
import com.bytedance.bitsail.common.column.LongColumn;
import com.bytedance.bitsail.common.column.MapColumn;
import com.bytedance.bitsail.common.column.StringColumn;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class RowBytesParserTest {

  private BitSailConfiguration adapterConf;
  private RowBytesParser rowBytesParser;

  @Before
  public void init() {
    adapterConf = BitSailConfiguration.newDefault();
    rowBytesParser = new RowBytesParser();
  }

  @Test
  public void testParseBitsailRow() {
    TypeInformation<?>[] typeInfos = {
        TypeInformation.of(String.class),   // 0
        TypeInformation.of(Boolean.class),
        TypeInformation.of(byte[].class),
        TypeInformation.of(Byte[].class),
        TypeInformation.of(Short.class),    // 4
        TypeInformation.of(Integer.class),
        TypeInformation.of(Long.class),
        TypeInformation.of(BigInteger.class),
        TypeInformation.of(Byte.class),    // 8
        TypeInformation.of(Date.class),
        TypeInformation.of(java.sql.Date.class),
        TypeInformation.of(java.sql.Time.class),
        TypeInformation.of(java.sql.Timestamp.class), // 12
        TypeInformation.of(Float.class),
        TypeInformation.of(Double.class),
        TypeInformation.of(BigDecimal.class),
        new ListTypeInfo<>(String.class), //16
        new MapTypeInfo<>(String.class, String.class)
    };
    RowTypeInfo rowTypeInfo = new RowTypeInfo(typeInfos);

    Row bitsailRow = new Row(18);
    bitsailRow.setField(0, new StringColumn("test_string"));
    bitsailRow.setField(1, new BooleanColumn(true));
    bitsailRow.setField(2, new BytesColumn(new byte[] {1, 2, 3}));
    bitsailRow.setField(3, new BytesColumn(new byte[] {1, 2, 3}));

    bitsailRow.setField(4, new LongColumn(1234L));
    bitsailRow.setField(5, new LongColumn(1234L));
    bitsailRow.setField(6, new LongColumn(1234L));
    bitsailRow.setField(7, new LongColumn(1234L));

    bitsailRow.setField(8, new LongColumn(1234L));
    bitsailRow.setField(9, new DateColumn(1672506061000L));
    bitsailRow.setField(10, new DateColumn(1672506061000L));
    bitsailRow.setField(11, new DateColumn(1672506061000L));

    bitsailRow.setField(12, new DateColumn(1672506061000L));
    bitsailRow.setField(13, new DoubleColumn(123.456d));
    bitsailRow.setField(14, new DoubleColumn(123.456d));
    bitsailRow.setField(15, new DoubleColumn(123.456d));

    bitsailRow.setField(16, new ListColumn<>(ImmutableList.of(
        new StringColumn("list_a"),
        new StringColumn("list_b"),
        new StringColumn("list_c")
    ), StringColumn.class));
    bitsailRow.setField(17, new MapColumn<>(ImmutableMap.of(
        new StringColumn("map_key_a"), new StringColumn("map_val_a"),
        new StringColumn("map_key_b"), new StringColumn("map_val_b"),
        new StringColumn("map_key_c"), new StringColumn("map_val_c")
    ), StringColumn.class, StringColumn.class));

    Row flinkRow = new Row(18);
    rowBytesParser.parseBitSailRow(flinkRow, bitsailRow, rowTypeInfo, adapterConf);

    Assert.assertEquals("test_string", flinkRow.getField(0));
    Assert.assertTrue((boolean) flinkRow.getField(1));
    Assert.assertArrayEquals(new byte[] {1, 2, 3}, (byte[]) flinkRow.getField(2));
    Assert.assertArrayEquals(new Byte[] {1, 2, 3}, (Byte[]) flinkRow.getField(3));

    Assert.assertEquals(Long.valueOf(1234).shortValue(), flinkRow.getField(4));
    Assert.assertEquals(Long.valueOf(1234).intValue(), flinkRow.getField(5));
    Assert.assertEquals(1234L, flinkRow.getField(6));
    Assert.assertEquals(BigInteger.valueOf(1234), flinkRow.getField(7));

    Assert.assertEquals(Long.valueOf(1234).byteValue(), flinkRow.getField(8));
    Assert.assertEquals(1672506061000L, ((Date) flinkRow.getField(9)).getTime());
    Assert.assertEquals(123.456f, flinkRow.getField(13));
    Assert.assertEquals(123.456d, flinkRow.getField(14));
    Assert.assertEquals(BigDecimal.valueOf(123.456d), flinkRow.getField(15));

    List<String> listField = (List<String>) flinkRow.getField(16);
    Assert.assertEquals(3, listField.size());
    Assert.assertTrue(listField.contains("list_a"));
    Assert.assertTrue(listField.contains("list_b"));
    Assert.assertTrue(listField.contains("list_c"));

    Map<String, String> mapField = (Map<String, String>) flinkRow.getField(17);
    Assert.assertEquals(3, mapField.size());
    Assert.assertEquals("map_val_a", mapField.get("map_key_a"));
    Assert.assertEquals("map_val_b", mapField.get("map_key_b"));
    Assert.assertEquals("map_val_c", mapField.get("map_key_c"));
  }
}
