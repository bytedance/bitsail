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

package com.bytedance.bitsail.connector.kudu.sink;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.model.ColumnInfo;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.connector.kudu.option.KuduWriterOptions;

import com.google.common.collect.Lists;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.PartialRow;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.sql.Date;
import java.util.List;

@SuppressWarnings("checkstyle:MagicNumber")
public class KuduRowBuilderTest {

  List<ColumnInfo> columnInfos = Lists.newArrayList(
      new ColumnInfo("key", "int"),
      new ColumnInfo("field_long", "long"),
      new ColumnInfo("field_double", "double"),
      new ColumnInfo("field_string", "string"),
      new ColumnInfo("field_date", "date"),
      new ColumnInfo("field_binary", "binary"),
      new ColumnInfo("field_boolean", "boolean")
  );
  List<ColumnSchema> columnSchemaList = Lists.newArrayList(
      new ColumnSchema.ColumnSchemaBuilder("key", Type.INT32).key(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("field_long", Type.INT64).build(),
      new ColumnSchema.ColumnSchemaBuilder("field_double", Type.DOUBLE).build(),
      new ColumnSchema.ColumnSchemaBuilder("field_string", Type.STRING).build(),
      new ColumnSchema.ColumnSchemaBuilder("field_date", Type.DATE).build(),
      new ColumnSchema.ColumnSchemaBuilder("field_binary", Type.BINARY).build(),
      new ColumnSchema.ColumnSchemaBuilder("field_boolean", Type.BOOL).build()
  );
  Schema schema = new Schema(columnSchemaList);

  @Test
  public void test() {
    BitSailConfiguration jobConf = BitSailConfiguration.newDefault();
    jobConf.set(KuduWriterOptions.COLUMNS, columnInfos);

    Row row = new Row(7);
    row.setField(0, 100);
    row.setField(1, 123456789L);
    row.setField(2, 12345.6789D);
    row.setField(3, "test_string");
    row.setField(4, Date.valueOf("2022-10-10"));
    row.setField(5, "test_binary".getBytes());
    row.setField(6, true);

    KuduRowBuilder rowBuilder = new KuduRowBuilder(jobConf, schema);
    PartialRow kuduRow = new PartialRow(schema);
    rowBuilder.build(kuduRow, row);

    Assert.assertEquals(100, kuduRow.getInt("key"));
    Assert.assertEquals(123456789L, kuduRow.getLong("field_long"));
    Assert.assertEquals(12345.6789D, kuduRow.getDouble("field_double"), 1e-6);
    Assert.assertEquals("test_string", kuduRow.getString("field_string"));
    Assert.assertEquals(Date.valueOf("2022-10-10"), kuduRow.getDate("field_date"));
    Assert.assertEquals(ByteBuffer.wrap("test_binary".getBytes()), kuduRow.getBinary("field_binary"));
    Assert.assertTrue(kuduRow.getBoolean("field_boolean"));
  }
}
