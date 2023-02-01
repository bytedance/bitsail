/*
 * Copyright 2022 Bytedance Ltd. and/or its affiliates.
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

import com.bytedance.bitsail.common.column.Column;
import com.bytedance.bitsail.common.column.ColumnCast;
import com.bytedance.bitsail.common.column.LongColumn;
import com.bytedance.bitsail.common.column.StringColumn;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.option.CommonOptions;
import com.bytedance.bitsail.flink.core.typeinfo.PrimitiveColumnTypeInfo;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BytesParserTest {

  private BytesParser bytesParser;

  @Before
  public void initBytesParser() {
    bytesParser = new BytesParser() {
      @Override
      public Row parse(Row row, byte[] bytes, int offset, int numBytes, String charsetName, RowTypeInfo rowTypeInfo) {
        return null;
      }

      @Override
      public Row parse(Row row, Object line, RowTypeInfo rowTypeInfo) throws Exception {
        return null;
      }
    };
  }

  @Test
  public void testStringColumn() {
    Object fieldVal;
    Column column;

    fieldVal = null;
    column = bytesParser.getStringColumnValue(fieldVal);
    Assert.assertNull(column.getRawData());

    Map<String, Object> map = new HashMap<>();
    map.put("test_key", "test_value");
    fieldVal = new JSONObject(map);
    column = bytesParser.getStringColumnValue(fieldVal);
    Assert.assertEquals(((JSONObject) fieldVal).toJSONString(), column.getRawData());

    fieldVal = 123456L;
    column = bytesParser.getStringColumnValue(fieldVal);
    Assert.assertEquals(fieldVal.toString(), column.getRawData());

    column = bytesParser.createBasicColumn(PrimitiveColumnTypeInfo.STRING_COLUMN_TYPE_INFO, fieldVal);
    Assert.assertEquals(fieldVal.toString(), column.getRawData());
  }

  @Test
  public void testBooleanColumn() {
    Object fieldVal;
    Column column;

    fieldVal = null;
    column = bytesParser.getBooleanColumnValue(fieldVal);
    Assert.assertFalse(column.asBoolean());

    fieldVal = Boolean.valueOf("true");
    column = bytesParser.getBooleanColumnValue(fieldVal);
    Assert.assertTrue(column.asBoolean());

    fieldVal = "true";
    column = bytesParser.getBooleanColumnValue(fieldVal);
    Assert.assertTrue(column.asBoolean());

    column = bytesParser.createBasicColumn(PrimitiveColumnTypeInfo.BOOL_COLUMN_TYPE_INFO, fieldVal);
    Assert.assertTrue(column.asBoolean());
  }

  @Test
  public void testDoubleColumn() {
    Object fieldVal;
    Column column;

    fieldVal = null;
    column = bytesParser.getDoubleColumnValue(fieldVal);
    Assert.assertNull(column.getRawData());

    fieldVal = BigDecimal.valueOf(123.45678d);
    column = bytesParser.getDoubleColumnValue(fieldVal);
    Assert.assertEquals(Double.valueOf(123.45678d).toString(), column.asString());

    fieldVal = 123.45678d;
    column = bytesParser.getDoubleColumnValue(fieldVal);
    Assert.assertEquals(Double.valueOf(123.45678d).toString(), column.asString());

    column = bytesParser.createBasicColumn(PrimitiveColumnTypeInfo.DOUBLE_COLUMN_TYPE_INFO, fieldVal);
    Assert.assertEquals(Double.valueOf(123.45678d).toString(), column.asString());
  }

  @Test
  public void testBytesColumn() {
    Object fieldVal;
    Column column;

    fieldVal = null;
    column = bytesParser.getBytesColumnValue(fieldVal);
    Assert.assertNull(column.getRawData());

    fieldVal = new byte[] {1, 2, 3, 4};
    column = bytesParser.getBytesColumnValue(fieldVal);
    Assert.assertArrayEquals((byte[]) fieldVal, column.asBytes());

    fieldVal = new Byte[] {1, 2, 3, 4};
    column = bytesParser.getBytesColumnValue(fieldVal);
    Assert.assertArrayEquals(new byte[] {1, 2, 3, 4}, column.asBytes());

    fieldVal = new byte[] {1, 2, 3, 4};
    column = bytesParser.getBytesColumnValue(new String((byte[]) fieldVal));
    Assert.assertArrayEquals((byte[]) fieldVal, column.asBytes());

    column = bytesParser.createBasicColumn(PrimitiveColumnTypeInfo.BYTES_COLUMN_TYPE_INFO, fieldVal);
    Assert.assertArrayEquals((byte[]) fieldVal, column.asBytes());
  }

  @Test
  public void testDateColumn() {
    BitSailConfiguration commonConf = BitSailConfiguration.newDefault();
    commonConf.set(CommonOptions.DateFormatOptions.TIME_ZONE, "UTC+8");
    ColumnCast.initColumnCast(commonConf);

    // 2023-01-01 00:00:00
    String date = "2023-01-01";
    String time = "01:59:00";
    String dateTime = "2023-01-01 01:59:00";
    Long timestamp = 1672509540000L;
    int year = 2023;
    int month = 1;
    int dayOfMonth = 1;
    int hour = 1;
    int minute = 59;

    Object fieldVal;
    Column column;

    fieldVal = null;
    column = bytesParser.getDateColumnValue(fieldVal);
    Assert.assertNull(column.getRawData());

    fieldVal = new java.sql.Date(timestamp);
    column = bytesParser.getDateColumnValue(fieldVal);
    Assert.assertEquals(date, column.asString());

    fieldVal = new java.sql.Time(timestamp);
    column = bytesParser.getDateColumnValue(fieldVal);
    Assert.assertEquals(time, column.asString());

    fieldVal = new java.sql.Timestamp(timestamp);
    column = bytesParser.getDateColumnValue(fieldVal);
    Assert.assertEquals(dateTime, column.asString());

    fieldVal = new Date(timestamp);
    column = bytesParser.getDateColumnValue(fieldVal);
    Assert.assertEquals(dateTime, column.asString());

    fieldVal = LocalDate.of(year, month, dayOfMonth);
    column = bytesParser.getDateColumnValue(fieldVal);
    Assert.assertEquals(java.sql.Date.valueOf(date).getTime(), column.asDate().getTime());

    fieldVal = LocalDateTime.of(year, month, dayOfMonth, hour, minute);
    column = bytesParser.getDateColumnValue(fieldVal);
    Assert.assertEquals(timestamp.longValue(), column.asDate().getTime());

    fieldVal = dateTime;
    column = bytesParser.getDateColumnValue(fieldVal);
    Assert.assertEquals(timestamp, column.asLong());

    fieldVal = LocalDateTime.of(year, month, dayOfMonth, hour, minute);
    column = bytesParser.createBasicColumn(PrimitiveColumnTypeInfo.DATE_COLUMN_TYPE_INFO, fieldVal);
    Assert.assertEquals(timestamp.longValue(), column.asDate().getTime());

    column = bytesParser.createBasicColumn(PrimitiveColumnTypeInfo.LONG_COLUMN_TYPE_INFO, fieldVal);
    Assert.assertEquals(timestamp.longValue(), column.asLong() * 1000L);
  }

  @Test
  public void testCreateBasicColumnNull() {
    Column column = bytesParser.createBasicColumnNull(StringColumn.class);
    Assert.assertTrue(column instanceof StringColumn);
    Assert.assertNull(column.getRawData());

    bytesParser.convertErrorColumnAsNull = true;
    column = bytesParser.createBasicColumn(PrimitiveColumnTypeInfo.LONG_COLUMN_TYPE_INFO, "abc");
    Assert.assertTrue(column instanceof LongColumn);
    Assert.assertNull(column.getRawData());
  }

  @Test
  public void testConvertArrayToList() {
    Object fieldValue;
    List<?> ret;

    fieldValue = new boolean[] {true, false, true};
    ret = bytesParser.convertArrayToList(fieldValue);
    Assert.assertArrayEquals(ret);
  }
}
