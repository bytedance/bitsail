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

import com.google.common.collect.Lists;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.KuduPredicate;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

@SuppressWarnings("checkstyle:MagicNumber")
public class KuduSourceSplitTest {

  List<ColumnSchema> columnSchemaList = Lists.newArrayList(
      new ColumnSchema.ColumnSchemaBuilder("key", Type.INT32).key(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("field_long", Type.INT64).build(),
      new ColumnSchema.ColumnSchemaBuilder("field_string", Type.STRING).build()
  );
  Schema schema = new Schema(columnSchemaList);

  @Test
  public void testComparisonPredicate() throws IOException {
    KuduSourceSplit split = new KuduSourceSplit(1);

    KuduPredicate lower = KuduPredicate.newComparisonPredicate(columnSchemaList.get(0),
        KuduPredicate.ComparisonOp.GREATER_EQUAL, 100);
    KuduPredicate upper = KuduPredicate.newComparisonPredicate(columnSchemaList.get(0),
        KuduPredicate.ComparisonOp.LESS, 1000);
    split.addPredicate(lower);
    split.addPredicate(upper);

    List<KuduPredicate> kuduPredicates = split.deserializePredicates(schema);

    Assert.assertEquals(2, kuduPredicates.size());
    for (KuduPredicate predicate : kuduPredicates) {
      Assert.assertTrue(lower.equals(predicate) || upper.equals(predicate));
    }
  }

  @Test
  public void testInListPredicate() throws IOException {
    KuduSourceSplit split = new KuduSourceSplit(1);

    KuduPredicate inListPred = KuduPredicate.newInListPredicate(columnSchemaList.get(2),
        Lists.newArrayList("hello", "world"));
    split.addPredicate(inListPred);

    List<KuduPredicate> kuduPredicates = split.deserializePredicates(schema);

    Assert.assertEquals(1, kuduPredicates.size());
    Assert.assertEquals(inListPred, kuduPredicates.get(0));
  }

  @Test
  public void testNullPredicate() throws IOException {
    KuduSourceSplit split = new KuduSourceSplit(1);

    KuduPredicate nonNullPred = KuduPredicate.newIsNotNullPredicate(columnSchemaList.get(2));
    split.addPredicate(nonNullPred);

    List<KuduPredicate> kuduPredicates = split.deserializePredicates(schema);

    Assert.assertEquals(1, kuduPredicates.size());
    Assert.assertEquals(nonNullPred, kuduPredicates.get(0));
  }
}
