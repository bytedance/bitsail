/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bytedance.bitsail.common.column;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class DateColumnTest {

  @Before
  public void before() {
    ColumnCast.initColumnCast(BitSailConfiguration.newDefault());
  }

  @Test
  public void testStringToDate() {
    String timeStr = "2019-04-01 11:11:11";
    StringColumn strColumn = new StringColumn(timeStr);

    DateColumn dateColumn = null;
    try {
      dateColumn = new DateColumn(ColumnCast.string2Date(strColumn));
    } catch (Exception e) {
    }

    assertEquals(dateColumn.asString(), timeStr);

  }

  @Test
  public void testStringToDateNoSep() {
    String timeStr = "20190401 11:11:11";
    String retStr = "2019-04-01 11:11:11";
    StringColumn strColumn = new StringColumn(timeStr);

    DateColumn dateColumn = null;
    try {
      dateColumn = new DateColumn(ColumnCast.string2Date(strColumn));
    } catch (Exception e) {
    }

    assertEquals(dateColumn.asString(), retStr);

  }

  @Test
  public void testErrorDate() {
    String timeStr = "2019---04011 11:11:11";
    StringColumn strColumn = new StringColumn(timeStr);

    DateColumn dateColumn = null;
    try {
      dateColumn = new DateColumn(ColumnCast.string2Date(strColumn));
      assertFalse("Invalid Date", true);
    } catch (Exception e) {
    }
  }

  @Test
  public void testStringToDate2() {
    String timeStr = "20190401";
    String retStr = "2019-04-01 00:00:00";
    StringColumn strColumn = new StringColumn(timeStr);

    DateColumn dateColumn = null;
    try {
      dateColumn = new DateColumn(ColumnCast.string2Date(strColumn));
    } catch (Exception e) {
    }

    assertEquals(dateColumn.asString(), retStr);

  }
}