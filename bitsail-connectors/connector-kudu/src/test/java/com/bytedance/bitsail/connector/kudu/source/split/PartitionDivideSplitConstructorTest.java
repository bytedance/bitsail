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

package com.bytedance.bitsail.connector.kudu.source.split;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.connector.kudu.option.KuduReaderOptions;
import com.bytedance.bitsail.connector.kudu.source.split.strategy.PartitionDivideSplitConstructor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduPredicate;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.Partition;
import org.apache.kudu.client.PartitionSchema;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("checkstyle:MagicNumber")
public class PartitionDivideSplitConstructorTest {

  List<ColumnSchema> columnSchemaList = Lists.newArrayList(
      new ColumnSchema.ColumnSchemaBuilder("id", Type.INT32).key(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("field_long", Type.INT64).build(),
      new ColumnSchema.ColumnSchemaBuilder("field_string", Type.STRING).build()
  );
  List<Integer> columnIds = Lists.newArrayList(0, 1, 2);
  Schema schema = new Schema(columnSchemaList, columnIds);

  String tableName = "test_kudu_table";
  KuduTable mockTable = Mockito.mock(KuduTable.class);
  KuduClient mockClient = Mockito.mock(KuduClient.class);

  List<KuduPredicate> lowerPredicates = Lists.newArrayList(
      KuduPredicate.newComparisonPredicate(columnSchemaList.get(0), KuduPredicate.ComparisonOp.GREATER_EQUAL, 100),
      KuduPredicate.newComparisonPredicate(columnSchemaList.get(0), KuduPredicate.ComparisonOp.GREATER_EQUAL, 400)
  );
  List<KuduPredicate> upperPredicates = Lists.newArrayList(
      KuduPredicate.newComparisonPredicate(columnSchemaList.get(0), KuduPredicate.ComparisonOp.LESS, 400),
      KuduPredicate.newComparisonPredicate(columnSchemaList.get(0), KuduPredicate.ComparisonOp.LESS, 700)
  );
  PartitionSchema.RangeSchema rangeSchema = new PartitionSchema.RangeSchema(Lists.newArrayList(0));
  PartitionSchema partitionSchema = new PartitionSchema(rangeSchema, new ArrayList <>(), schema);
  @Before
  public void init() throws Exception {
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

    Mockito.when(mockTable.getSchema()).thenReturn(schema);
    Mockito.when(mockClient.openTable(tableName)).thenReturn(mockTable);
    Mockito.when(mockTable.getPartitionSchema()).thenReturn(partitionSchema);
    Mockito.when(mockTable.getRangePartitions(3000L)).thenReturn(partitions);
  }

  @Test
  public void testParseSplitConf() throws Exception {
    PartitionDivideSplitConstructor.SplitConfiguration splitConf = new PartitionDivideSplitConstructor.SplitConfiguration();
    splitConf.setSplitNum(3);
    BitSailConfiguration jobConf = BitSailConfiguration.newDefault();
    jobConf.set(KuduReaderOptions.SPLIT_STRATEGY, "partition_divide");
    jobConf.set(KuduReaderOptions.KUDU_TABLE_NAME, tableName);
    jobConf.set(KuduReaderOptions.SPLIT_CONFIGURATION, new ObjectMapper().writeValueAsString(splitConf));
    jobConf.set(KuduReaderOptions.READER_PARALLELISM_NUM, 3);

    PartitionDivideSplitConstructor constructor = new PartitionDivideSplitConstructor(jobConf, mockClient);
    Assert.assertEquals(2, constructor.estimateSplitNum());

    List<KuduSourceSplit> splits = constructor.construct(mockClient);
    Assert.assertEquals(2, splits.size());

    for (int i = 0; i < splits.size(); ++i) {
      List<KuduPredicate> predicates = splits.get(i).deserializePredicates(schema);
      Assert.assertEquals(2, predicates.size());
      Assert.assertEquals(lowerPredicates.get(i), predicates.get(0));
      Assert.assertEquals(upperPredicates.get(i), predicates.get(1));
    }

  }
}
