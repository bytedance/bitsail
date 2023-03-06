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

package com.bytedance.bitsail.common.column;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.option.CommonOptions;

import org.junit.Before;
import org.junit.Test;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;

import static org.junit.Assert.assertEquals;

public class StringColumnTest {

  private String timeZone;

  private final String[] dateStrList = {
      "2023-01-01",
      "2023-01-01 12:34:56",
      "12:34:56"
  };
  private final String[] expectedDateStrList = {
      "2023-01-01 00:00:00",
      "2023-01-01 12:34:56",
      "1970-01-01 12:34:56"
  };
  private final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

  @Before
  public void before() {
    timeZone = ZoneOffset.ofHours(0).getId();
    BitSailConfiguration bitSailConfiguration = BitSailConfiguration.newDefault();
    bitSailConfiguration.set(CommonOptions.DateFormatOptions.TIME_ZONE, timeZone);
    ColumnCast.refresh();
    ColumnCast.initColumnCast(bitSailConfiguration);
  }

  @Test
  public void testParseDate() {
    for (int i = 0; i < dateStrList.length; ++i) {
      StringColumn column = new StringColumn(dateStrList[i]);
      Date date = column.asDate();
      String parsedDateStr = formatter.format(date.toInstant()
          .atZone(ZoneId.of(timeZone)).toOffsetDateTime());
      assertEquals(expectedDateStrList[i], parsedDateStr);
    }
  }

  @Test
  public void testParseLocalDateTime() {
    for (int i = 0; i < dateStrList.length; ++i) {
      StringColumn column = new StringColumn(dateStrList[i]);
      LocalDateTime parsedLocalDateTime = column.asLocalDateTime();
      assertEquals(expectedDateStrList[i], parsedLocalDateTime.format(formatter));
    }
  }

  @Test
  public void testAsBytes() {
    String str = "abc";
    StringColumn sc = new StringColumn(str);
    assertEquals(str, new String(sc.asBytes()));

    str = "";
    sc = new StringColumn(str);
    assertEquals(str, new String(sc.asBytes()));
  }
}