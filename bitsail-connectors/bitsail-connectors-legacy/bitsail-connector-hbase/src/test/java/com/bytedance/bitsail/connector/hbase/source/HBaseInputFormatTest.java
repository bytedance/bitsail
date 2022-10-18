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

package com.bytedance.bitsail.connector.hbase.source;

import com.bytedance.bitsail.common.column.DoubleColumn;
import com.bytedance.bitsail.common.column.LongColumn;
import com.bytedance.bitsail.common.column.StringColumn;
import com.bytedance.bitsail.common.model.ColumnInfo;
import com.bytedance.bitsail.flink.core.typeutils.ColumnFlinkTypeInfoUtil;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class HBaseInputFormatTest {

  @Test
  public void testHBaseRowTypeInfo() {
    List<ColumnInfo> columnInfos = new ArrayList<>();
    columnInfos.add(new ColumnInfo("str_field", "string"));
    columnInfos.add(new ColumnInfo("int_field", "int"));
    columnInfos.add(new ColumnInfo("double_field", "double"));
    RowTypeInfo rowTypeInfo = ColumnFlinkTypeInfoUtil.getRowTypeInformation("hbase", columnInfos);

    Assert.assertTrue(rowTypeInfo.getTypeAt(0).getTypeClass().isAssignableFrom(StringColumn.class));
    Assert.assertTrue(rowTypeInfo.getTypeAt(1).getTypeClass().isAssignableFrom(LongColumn.class));
    Assert.assertTrue(rowTypeInfo.getTypeAt(2).getTypeClass().isAssignableFrom(DoubleColumn.class));
  }
}
