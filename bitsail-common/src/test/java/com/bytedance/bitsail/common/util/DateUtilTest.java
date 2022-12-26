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

package com.bytedance.bitsail.common.util;

import com.bytedance.bitsail.common.column.StringColumn;

import org.junit.Assert;
import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUtilTest {
  @Test
  public void testConvertStringToSeconds() {
    Assert.assertEquals(1575734400L, DateUtil.convertStringToSeconds(new StringColumn("1575734400")));
    Assert.assertEquals(1575734400L, DateUtil.convertStringToSeconds(new StringColumn("1575734400000")));
    Assert.assertEquals(1575734400L, DateUtil.convertStringToSeconds(new StringColumn("2020-08-01")));
    Assert.assertEquals(1596214923L, DateUtil.convertStringToSeconds(new StringColumn("2020-08-01 01:02:03")));
  }

  @Test
  public void testGetDatesBetweenTwoDate(){
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    try {
      Date dStart = sdf.parse("2022-12-26");
      Date dEnd = sdf.parse("2022-12-26");
      Assert.assertEquals(1, DateUtil.getDatesBetweenTwoDate(dStart, dEnd).size());
    } catch (ParseException e) {
      e.printStackTrace();
    }
  }
  @Test
  public void testGetNDaysAfterDate(){
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    try {
      Date date = sdf.parse("2022-12-31");
      Assert.assertEquals("2023-01-01", sdf.format(DateUtil.getNDaysAfterDate(date, 1)));
    } catch (ParseException e) {
      e.printStackTrace();
    }
  }
}
