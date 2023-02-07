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
import com.bytedance.bitsail.connector.kudu.source.split.strategy.SimpleDivideSplitConstructor;
import com.bytedance.bitsail.connector.kudu.source.split.strategy.SimpleDivideSplitConstructor.SplitConfiguration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduPredicate;
import org.apache.kudu.client.KuduTable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.List;

@SuppressWarnings("checkstyle:MagicNumber")
public class SimpleDivideSplitConstructorTest {

  List<ColumnSchema> columnSchemaList = Lists.newArrayList(
      new ColumnSchema.ColumnSchemaBuilder("key", Type.INT32).key(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("field_long", Type.INT64).build(),
      new ColumnSchema.ColumnSchemaBuilder("field_string", Type.STRING).build()
  );
  Schema schema = new Schema(columnSchemaList);

  String tableName = "test_kudu_table";
  KuduTable mockTable = Mockito.mock(KuduTable.class);
  KuduClient mockClient = Mockito.mock(KuduClient.class);

  @Before
  public void init() throws Exception {
    Mockito.when(mockTable.getSchema()).thenReturn(schema);
    Mockito.when(mockClient.openTable(tableName)).thenReturn(mockTable);
  }

  @Test
  public void testParseSplitConf() throws Exception {
    SplitConfiguration splitConf = new SplitConfiguration();
    splitConf.setName("key");
    splitConf.setLower(0L);
    splitConf.setUpper(1000L);
    splitConf.setSplitNum(3);

    BitSailConfiguration jobConf = BitSailConfiguration.newDefault();
    jobConf.set(KuduReaderOptions.KUDU_TABLE_NAME, tableName);
    jobConf.set(KuduReaderOptions.SPLIT_CONFIGURATION, new ObjectMapper().writeValueAsString(splitConf));

    SimpleDivideSplitConstructor constructor = new SimpleDivideSplitConstructor(jobConf, mockClient);
    Assert.assertEquals(3, constructor.estimateSplitNum());

    List<KuduSourceSplit> splits = constructor.construct(mockClient);
    Assert.assertEquals(3, splits.size());

    List<KuduPredicate> lowerPredicates = Lists.newArrayList(
        KuduPredicate.newComparisonPredicate(columnSchemaList.get(0), KuduPredicate.ComparisonOp.GREATER_EQUAL, 0),
        KuduPredicate.newComparisonPredicate(columnSchemaList.get(0), KuduPredicate.ComparisonOp.GREATER_EQUAL, 334),
        KuduPredicate.newComparisonPredicate(columnSchemaList.get(0), KuduPredicate.ComparisonOp.GREATER_EQUAL, 668)
    );
    List<KuduPredicate> upperPredicates = Lists.newArrayList(
        KuduPredicate.newComparisonPredicate(columnSchemaList.get(0), KuduPredicate.ComparisonOp.LESS, 334),
        KuduPredicate.newComparisonPredicate(columnSchemaList.get(0), KuduPredicate.ComparisonOp.LESS, 668),
        KuduPredicate.newComparisonPredicate(columnSchemaList.get(0), KuduPredicate.ComparisonOp.LESS, 1002)
    );

    for (int i = 0; i < 3; ++i) {
      List<KuduPredicate> predicates = splits.get(i).deserializePredicates(schema);
      Assert.assertEquals(2, predicates.size());
      Assert.assertTrue(lowerPredicates.get(i).equals(predicates.get(0)) || lowerPredicates.get(i).equals(predicates.get(1)));
      Assert.assertTrue(upperPredicates.get(i).equals(predicates.get(0)) || upperPredicates.get(i).equals(predicates.get(1)));
    }
  }

  @Test
  public void testParsePartialSplitConf() throws Exception {
    SplitConfiguration splitConf = new SplitConfiguration();
    splitConf.setName("key");
    splitConf.setLower(0L);
    splitConf.setUpper(1000L);

    BitSailConfiguration jobConf = BitSailConfiguration.newDefault();
    jobConf.set(KuduReaderOptions.KUDU_TABLE_NAME, tableName);
    jobConf.set(KuduReaderOptions.SPLIT_CONFIGURATION, new ObjectMapper().writeValueAsString(splitConf));
    jobConf.set(KuduReaderOptions.READER_PARALLELISM_NUM, 3);

    SimpleDivideSplitConstructor constructor = new SimpleDivideSplitConstructor(jobConf, mockClient);
    Assert.assertEquals(3, constructor.estimateSplitNum());
  }
}
