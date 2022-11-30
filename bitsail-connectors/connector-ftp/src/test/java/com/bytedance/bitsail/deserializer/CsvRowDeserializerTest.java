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
import com.bytedance.bitsail.connector.ftp.option.FtpReaderOptions;
import com.bytedance.bitsail.connector.ftp.source.reader.deserializer.CsvRowDeserializer;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class CsvRowDeserializerTest {

  private static List<ColumnInfo> getColumnInfo(String columnName1, String type1, String columnName2, String type2) {
    List<ColumnInfo> columnInfos = Lists.newArrayListWithExpectedSize(2);
    columnInfos.add(new ColumnInfo(columnName1, type1));
    columnInfos.add(new ColumnInfo(columnName2, type2));
    return columnInfos;
  }

  @Test
  public void testCsvDeltmiter() throws IOException {
    BitSailConfiguration jobConf = BitSailConfiguration.newDefault();
    jobConf.set(FtpReaderOptions.CSV_DELIMITER, "#");
    jobConf.set(FtpReaderOptions.COLUMNS, getColumnInfo("c1", "int", "c2", "string"));
    CsvRowDeserializer parser = new CsvRowDeserializer(jobConf);
    Row row = parser.convert("1111#aaa");
    Assert.assertEquals((int) row.getField(0), 1111);
    Assert.assertEquals(row.getField(1).toString(), "aaa");
  }

  @Test
  public void testCsvQuote() throws IOException {
    BitSailConfiguration jobConf = BitSailConfiguration.newDefault();
    jobConf.set(FtpReaderOptions.CSV_DELIMITER, "#");
    jobConf.set(FtpReaderOptions.CSV_ESCAPE, '\\');
    jobConf.set(FtpReaderOptions.CSV_QUOTE, '\"');
    jobConf.set(FtpReaderOptions.COLUMNS, getColumnInfo("c1", "string", "c2", "int"));
    CsvRowDeserializer parser = new CsvRowDeserializer(jobConf);
    Row row = parser.convert("\"star\"#2222");
    Assert.assertEquals(row.getField(0).toString(), "star");
    Assert.assertEquals((int) row.getField(1), 2222);
  }

  @Test
  public void testConvertErrorColumnAsNull() throws IOException {
    BitSailConfiguration jobConf = BitSailConfiguration.newDefault();
    jobConf.set(FtpReaderOptions.CONVERT_ERROR_COLUMN_AS_NULL, true);
    jobConf.set(FtpReaderOptions.COLUMNS, getColumnInfo("c1", "int", "c2", "int"));
    CsvRowDeserializer parser = new CsvRowDeserializer(jobConf);
    Row row = parser.convert("1111,aaa");
    Assert.assertNull(row.getField(1));
  }

  @Test
  public void testConvertTimeStamp() throws IOException {
    BitSailConfiguration jobConf = BitSailConfiguration.newDefault();
    jobConf.set(FtpReaderOptions.CONVERT_ERROR_COLUMN_AS_NULL, true);
    jobConf.set(FtpReaderOptions.COLUMNS, getColumnInfo("c1", "timestamp", "c2", "timestamp"));
    CsvRowDeserializer parser = new CsvRowDeserializer(jobConf);
    Row row = parser.convert("1669705101,2022-01-01 23:23:23");
    Assert.assertNotNull(row.getField(0));
    Assert.assertNotNull(row.getField(1));
  }

  @Test
  public void testConvertDate() throws IOException {
    BitSailConfiguration jobConf = BitSailConfiguration.newDefault();
    jobConf.set(FtpReaderOptions.CONVERT_ERROR_COLUMN_AS_NULL, true);
    jobConf.set(FtpReaderOptions.COLUMNS, getColumnInfo("c1", "date", "c2", "date"));
    CsvRowDeserializer parser = new CsvRowDeserializer(jobConf);
    Row row1 = parser.convert("2022-01-01,2022-01-01 23:23:23");
    Assert.assertNotNull(row1.getField(0));
    Assert.assertNotNull(row1.getField(1));
    Row row2 = parser.convert("20220101,20220102232323");
    Assert.assertNotNull(row2.getField(0));
    Assert.assertNotNull(row2.getField(1));
  }
}
