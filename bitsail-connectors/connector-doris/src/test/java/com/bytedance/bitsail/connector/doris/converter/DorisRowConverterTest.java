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

package com.bytedance.bitsail.connector.doris.converter;

import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.connector.doris.typeinfo.DorisDataType;

import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.TimeZone;

public class DorisRowConverterTest {

  private static final long CURRENT_MILLS = System.currentTimeMillis();
  private final DorisDataType[] types = {
      DorisDataType.NULL,
      DorisDataType.CHAR,
      DorisDataType.VARCHAR,
      DorisDataType.TEXT,
      DorisDataType.BOOLEAN,
      DorisDataType.BINARY,
      DorisDataType.VARBINARY,
      DorisDataType.DECIMAL,
      DorisDataType.DECIMALV2,
      DorisDataType.TINYINT,
      DorisDataType.SMALLINT,
      DorisDataType.INT,
      DorisDataType.INTEGER,
      DorisDataType.INTERVAL_YEAR_MONTH,
      DorisDataType.INTERVAL_DAY_TIME,
      DorisDataType.BIGINT,
      DorisDataType.LARGEINT,
      DorisDataType.FLOAT,
      DorisDataType.DOUBLE,
      DorisDataType.DATE,
      DorisDataType.DATETIME,
      DorisDataType.TIMESTAMP_WITHOUT_TIME_ZONE,
      DorisDataType.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
      DorisDataType.TIMESTAMP_WITH_TIME_ZONE};

  private final Object[] fieldValues = {null, "c", "VARCHAR", "TEXT", true, "binary".getBytes(), "varbinary".getBytes(),
      new BigDecimal("102921.2312314"), new BigDecimal("102921.2312314"), Short.MAX_VALUE,
      Short.MAX_VALUE, 20, 20, 20, 20, BigInteger.valueOf(Long.MAX_VALUE), BigInteger.valueOf(Long.MAX_VALUE), Float.MAX_VALUE, Double.MAX_VALUE,
      new Date(CURRENT_MILLS),
      Timestamp.valueOf(LocalDateTime.ofInstant(Instant.ofEpochSecond(CURRENT_MILLS / 1000), TimeZone.getDefault().toZoneId())),
      Timestamp.valueOf(LocalDateTime.ofInstant(Instant.ofEpochSecond(CURRENT_MILLS / 1000), TimeZone.getDefault().toZoneId())),
      Timestamp.valueOf(LocalDateTime.ofInstant(Instant.ofEpochSecond(CURRENT_MILLS / 1000), TimeZone.getDefault().toZoneId())),
      Timestamp.valueOf(LocalDateTime.ofInstant(Instant.ofEpochSecond(CURRENT_MILLS / 1000), TimeZone.getTimeZone("Asia/Shanghai").toZoneId()))};

  public Row getDtsRow() {
    int columnSize = fieldValues.length;
    Row dtsRow = new Row(columnSize);
    for (int i = 0; i < columnSize; i++) {
      dtsRow.setField(i, fieldValues[i]);
    }
    return dtsRow;
  }

  @Test
  public void testConvertExternal() {
    DorisRowConverter converter = new DorisRowConverter(types);
    Row dtsRow = getDtsRow();
    int columnSize = dtsRow.getArity();
    for (int i = 0; i < columnSize; i++) {
      Object convertedValue = converter.convertExternal(dtsRow, i);
      Assert.assertEquals(fieldValues[i], convertedValue);
    }
  }

}
