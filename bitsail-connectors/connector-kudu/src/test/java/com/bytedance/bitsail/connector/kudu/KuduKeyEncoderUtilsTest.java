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

package com.bytedance.bitsail.connector.kudu;

import com.bytedance.bitsail.connector.kudu.util.KuduKeyEncoderUtils;

import com.google.common.collect.Lists;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.Partition;
import org.apache.kudu.client.PartitionSchema;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("checkstyle:MagicNumber")
public class KuduKeyEncoderUtilsTest {
  List <ColumnSchema> columnSchemaList = Lists.newArrayList(
      new ColumnSchema.ColumnSchemaBuilder("id", Type.INT32).key(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("field_long", Type.INT64).build(),
      new ColumnSchema.ColumnSchemaBuilder("field_string", Type.STRING).build()
  );
  List<Integer> columnIds = Lists.newArrayList(0, 1, 2);
  Schema schema = new Schema(columnSchemaList, columnIds);

  @Test
  public void testDecodeRangePartitionKey() {
    PartialRow lowerPartialRow1 = new PartialRow(schema);
    lowerPartialRow1.addInt(0, 100);
    PartialRow upperPartialRow1 = new PartialRow(schema);
    upperPartialRow1.addInt(0, 400);

    PartialRow lowerPartialRow2 = new PartialRow(schema);
    lowerPartialRow2.addInt(0, 400);
    PartialRow upperPartialRow2 = new PartialRow(schema);
    upperPartialRow2.addInt(0, 700);

    Partition partition1 = new Partition(lowerPartialRow1.encodePrimaryKey(), upperPartialRow1.encodePrimaryKey(), Lists.newArrayList());
    Partition partition2 = new Partition(lowerPartialRow2.encodePrimaryKey(), upperPartialRow2.encodePrimaryKey(), Lists.newArrayList());
    List<Partition> partitions = Lists.newArrayList(partition1, partition2);
    PartitionSchema.RangeSchema rangeSchema = new PartitionSchema.RangeSchema(Lists.newArrayList(0));
    PartitionSchema partitionSchema = new PartitionSchema(rangeSchema, new ArrayList <>(), schema);

    PartialRow[][] partialRows = new PartialRow[][] {{lowerPartialRow1, upperPartialRow1}, {lowerPartialRow2, upperPartialRow2}};

    for (int i = 0; i < partitions.size(); i++) {
      Assert.assertEquals(partialRows[i][0].getInt(0), KuduKeyEncoderUtils.decodeRangePartitionKey(schema, partitionSchema,
          ByteBuffer.wrap(partitions.get(i).getPartitionKeyStart()).order(ByteOrder.BIG_ENDIAN)).getInt(0));
      Assert.assertEquals(partialRows[i][1].getInt(0), KuduKeyEncoderUtils.decodeRangePartitionKey(schema, partitionSchema,
          ByteBuffer.wrap(partitions.get(i).getPartitionKeyEnd()).order(ByteOrder.BIG_ENDIAN)).getInt(0));
    }
  }
}
