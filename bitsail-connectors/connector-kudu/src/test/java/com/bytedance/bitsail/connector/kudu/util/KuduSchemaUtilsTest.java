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

package com.bytedance.bitsail.connector.kudu.util;

import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.model.ColumnInfo;
import com.bytedance.bitsail.connector.kudu.util.KuduSchemaUtils;

import com.google.common.collect.Lists;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.KuduTable;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.List;

public class KuduSchemaUtilsTest {

  List<ColumnInfo> columnInfos = Lists.newArrayList(
      new ColumnInfo("key", "int"),
      new ColumnInfo("field_long", "long"),
      new ColumnInfo("field_varchar", "string")
  );
  List<ColumnSchema> columnSchemaList = Lists.newArrayList(
      new ColumnSchema.ColumnSchemaBuilder("key", Type.INT32).key(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("field_long", Type.INT64).build(),
      new ColumnSchema.ColumnSchemaBuilder("field_string", Type.STRING).build()
  );
  Schema schema = new Schema(columnSchemaList);

  @Test(expected = BitSailException.class)
  public void testCheckColumnsExist() {
    KuduTable mockTable = Mockito.mock(KuduTable.class);
    Mockito.when(mockTable.getSchema()).thenReturn(schema);

    KuduSchemaUtils.checkColumnsExist(mockTable, columnInfos);
  }
}
