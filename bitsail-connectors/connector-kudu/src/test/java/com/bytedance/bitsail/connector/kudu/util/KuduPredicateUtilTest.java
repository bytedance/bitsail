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

import com.google.common.collect.Lists;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.ColumnTypeAttributes;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.KuduPredicate;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class KuduPredicateUtilTest {
  private static List<ColumnSchema> columnSchemaList = Lists.newArrayList(
      new ColumnSchema.ColumnSchemaBuilder("key", Type.INT32).key(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("field_long", Type.INT64).build(),
      new ColumnSchema.ColumnSchemaBuilder("field_string", Type.STRING).build(),
      new ColumnSchema.ColumnSchemaBuilder("field_float", Type.FLOAT).build(),
      new ColumnSchema.ColumnSchemaBuilder("field_double", Type.DOUBLE).build(),
      new ColumnSchema.ColumnSchemaBuilder("field_binary", Type.BINARY).build(),
      new ColumnSchema.ColumnSchemaBuilder("field_varchar", Type.VARCHAR).typeAttributes(
          new ColumnTypeAttributes.ColumnTypeAttributesBuilder().length(10).build()).build(),
      new ColumnSchema.ColumnSchemaBuilder("field_date", Type.DATE).build(),
      new ColumnSchema.ColumnSchemaBuilder("field_timestamp", Type.UNIXTIME_MICROS).build(),
      new ColumnSchema.ColumnSchemaBuilder("field_boolean", Type.BOOL).build()

  );
  private static Schema schema = new Schema(columnSchemaList);
  @Test
  public void testNormalParseFromConfig() {
    String json = "[\"AND\", [\">=\", \"key\", 1000], [\"IN\", \"key\", [999, 1001, 1003, 1005, 1007, 1009]], " +
        "[\"NULL\", \"field_varchar\"], [\"NOTNULL\",\"field_binary\"]]";
    List<KuduPredicate> predicates = KuduPredicateUtil.parseFromConfig(json, schema);
    Assert.assertEquals(4, predicates.size());
    Assert.assertTrue(predicates.get(0).equals(KuduPredicate.newComparisonPredicate(columnSchemaList.get(0), KuduPredicate.ComparisonOp.GREATER_EQUAL, 1000)));
    Assert.assertTrue(predicates.get(1).equals(KuduPredicate.newInListPredicate(columnSchemaList.get(0), Arrays.asList(999, 1001, 1003, 1005, 1007, 1009))));
    Assert.assertTrue(predicates.get(2).equals(KuduPredicate.newIsNullPredicate(columnSchemaList.get(6))));
    Assert.assertTrue(predicates.get(3).equals(KuduPredicate.newIsNotNullPredicate(columnSchemaList.get(5))));
  }

  @Test
  public void testInPredicateOperandFromConfig() {
    String json = "[\"AND\", [\"IN\", \"field_boolean\", [false]]]";
    List<KuduPredicate> predicates = KuduPredicateUtil.parseFromConfig(json, schema);
    Assert.assertEquals(1, predicates.size());
    Assert.assertTrue(predicates.get(0).equals(KuduPredicate.newInListPredicate(columnSchemaList.get(9), Collections.singletonList(false))));

    json = "[\"AND\", [\"IN\", \"field_long\", [1, 2, 3]]]";
    predicates = KuduPredicateUtil.parseFromConfig(json, schema);
    Assert.assertEquals(1, predicates.size());
    Assert.assertTrue(predicates.get(0).equals(KuduPredicate.newInListPredicate(columnSchemaList.get(1), Arrays.asList(1L, 2L, 3L))));

    json = "[\"AND\", [\"IN\", \"field_timestamp\", [1688004522000, 1688004523000, 1688004524000]]]";
    predicates = KuduPredicateUtil.parseFromConfig(json, schema);
    Assert.assertEquals(1, predicates.size());
    Assert.assertTrue(predicates.get(0).equals(KuduPredicate.newInListPredicate(columnSchemaList.get(8),
        Arrays.asList(1688004522000L, 1688004523000L, 1688004524000L))));

    json = "[\"AND\", [\"IN\", \"field_string\", [\"abc\", \"123\"]]]";
    predicates = KuduPredicateUtil.parseFromConfig(json, schema);
    Assert.assertEquals(1, predicates.size());
    Assert.assertTrue(predicates.get(0).equals(KuduPredicate.newInListPredicate(columnSchemaList.get(2),
        Arrays.asList("abc", "123"))));
  }

  @Test(expected = BitSailException.class)
  public void testInPredicateOperandFromConfigUnixTimestampException() {
    String json = "[\"AND\", [\"IN\", \"field_timestamp\", [\"2023-06-29 12:00:01.123456\", \"2023-06-29 13:05:01.023456\"]]]";
    List<KuduPredicate> predicates = KuduPredicateUtil.parseFromConfig(json, schema);
    Assert.assertEquals(1, predicates.size());
    Assert.assertTrue(predicates.get(0).equals(KuduPredicate.newInListPredicate(columnSchemaList.get(8),
        Arrays.asList(1688004522000L, 1688004523000L, 1688004524000L))));
  }

  @Test(expected = BitSailException.class)
  public void testInPredicateOperandFromConfigDateException() {
    String json = "[\"AND\", [\"IN\", \"field_date\", [1688004522000, 1688004523000, 1688004524000]]]";
    List<KuduPredicate> predicates = KuduPredicateUtil.parseFromConfig(json, schema);
    Assert.assertEquals(1, predicates.size());
    Assert.assertTrue(predicates.get(0).equals(KuduPredicate.newInListPredicate(columnSchemaList.get(7),
        Arrays.asList(1688004522000L, 1688004523000L, 1688004524000L))));
  }
}
