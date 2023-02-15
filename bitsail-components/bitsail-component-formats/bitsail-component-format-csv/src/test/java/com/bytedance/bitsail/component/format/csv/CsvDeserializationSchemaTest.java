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

package com.bytedance.bitsail.component.format.csv;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.common.typeinfo.RowTypeInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfos;
import com.bytedance.bitsail.component.format.csv.option.CsvReaderOptions;

import org.junit.Assert;
import org.junit.Test;

public class CsvDeserializationSchemaTest {

  @Test
  public void testConvertErrorColumnAsNull() {
    BitSailConfiguration jobConf = BitSailConfiguration.newDefault();
    jobConf.set(CsvReaderOptions.CONVERT_ERROR_COLUMN_AS_NULL, true);
    TypeInfo<?>[] typeInfos = {TypeInfos.INT_TYPE_INFO};
    String[] fieldNames = {"c1"};
    String csv = "aaa";
    RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldNames, typeInfos);
    CsvDeserializationSchema deserializationSchema = new CsvDeserializationSchema(jobConf, rowTypeInfo);
    Row row = deserializationSchema.deserialize(csv.getBytes());
    Assert.assertNull(row.getField(0));
  }

  @Test
  public void testCsvDelimiter() {
    BitSailConfiguration jobConf = BitSailConfiguration.newDefault();
    jobConf.set(CsvReaderOptions.CSV_DELIMITER, "#");
    TypeInfo<?>[] typeInfos = {TypeInfos.STRING_TYPE_INFO, TypeInfos.INT_TYPE_INFO};
    String[] fieldNames = {"c1", "c2"};
    String csv = "aaa#111";
    RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldNames, typeInfos);
    CsvDeserializationSchema deserializationSchema = new CsvDeserializationSchema(jobConf, rowTypeInfo);
    Row row = deserializationSchema.deserialize(csv.getBytes());
    Assert.assertEquals(row.getField(0).toString(), "aaa");
    Assert.assertEquals((int) row.getField(1), 111);
  }

  @Test
  public void testCsvQuote() {
    BitSailConfiguration jobConf = BitSailConfiguration.newDefault();
    jobConf.set(CsvReaderOptions.CSV_DELIMITER, "#");
    jobConf.set(CsvReaderOptions.CSV_ESCAPE, '\\');
    jobConf.set(CsvReaderOptions.CSV_QUOTE, '\"');
    TypeInfo<?>[] typeInfos = {TypeInfos.STRING_TYPE_INFO, TypeInfos.INT_TYPE_INFO};
    String[] fieldNames = {"c1", "c2"};
    String csv = "\"star\"#2222";
    RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldNames, typeInfos);
    CsvDeserializationSchema deserializationSchema = new CsvDeserializationSchema(jobConf, rowTypeInfo);
    Row row = deserializationSchema.deserialize(csv.getBytes());
    Assert.assertEquals(row.getField(0).toString(), "star");
    Assert.assertEquals((int) row.getField(1), 2222);
  }

  @Test
  public void testCsvMultiDelimiterReplaceChar() {
    BitSailConfiguration jobConf = BitSailConfiguration.newDefault();
    jobConf.set(CsvReaderOptions.CSV_DELIMITER, "##");
    TypeInfo<?>[] typeInfos = {TypeInfos.STRING_TYPE_INFO, TypeInfos.INT_TYPE_INFO};
    String[] fieldNames = {"c1", "c2"};
    String csv = "star##2222";
    RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldNames, typeInfos);
    CsvDeserializationSchema deserializationSchema = new CsvDeserializationSchema(jobConf, rowTypeInfo);
    Row row = deserializationSchema.deserialize(csv.getBytes());
    Assert.assertEquals(row.getField(0).toString(), "star");
    Assert.assertEquals((int) row.getField(1), 2222);
  }
}
