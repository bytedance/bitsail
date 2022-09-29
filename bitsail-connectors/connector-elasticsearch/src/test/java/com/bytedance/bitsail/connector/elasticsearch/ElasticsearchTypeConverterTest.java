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

package com.bytedance.bitsail.connector.elasticsearch;

import com.bytedance.bitsail.common.ddl.typeinfo.TypeInfo;
import com.bytedance.bitsail.common.type.BaseEngineTypeInfoConverter;
import com.bytedance.bitsail.common.util.Pair;
import com.bytedance.bitsail.connector.elasticsearch.sink.ElasticsearchWriterGenerator;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static com.bytedance.bitsail.common.ddl.typeinfo.TypeFactory.BIG_INT_TYPE_INFO;
import static com.bytedance.bitsail.common.ddl.typeinfo.TypeFactory.BOOLEAN_TYPE_INFO;
import static com.bytedance.bitsail.common.ddl.typeinfo.TypeFactory.BYTE_PRIMITIVE_ARRAY_TYPE_INFO;
import static com.bytedance.bitsail.common.ddl.typeinfo.TypeFactory.DATE;
import static com.bytedance.bitsail.common.ddl.typeinfo.TypeFactory.DOUBLE_TYPE_INFO;
import static com.bytedance.bitsail.common.ddl.typeinfo.TypeFactory.INT_TYPE_INFO;
import static com.bytedance.bitsail.common.ddl.typeinfo.TypeFactory.STRING_TYPE_INFO;

public class ElasticsearchTypeConverterTest {

  private final List<Pair<TypeInfo<?>, String>> toBitSailTypePairs = Arrays.asList(
      Pair.newPair(BIG_INT_TYPE_INFO, "bigint"),
      Pair.newPair(DOUBLE_TYPE_INFO, "double"),
      Pair.newPair(BIG_INT_TYPE_INFO, "timestamp"),
      Pair.newPair(DATE, "date"),
      Pair.newPair(STRING_TYPE_INFO, "varchar"),
      Pair.newPair(BOOLEAN_TYPE_INFO, "boolean"),
      Pair.newPair(BYTE_PRIMITIVE_ARRAY_TYPE_INFO, "binary"),
      Pair.newPair(STRING_TYPE_INFO, "text"),
      Pair.newPair(STRING_TYPE_INFO, "keyword"),
      Pair.newPair(INT_TYPE_INFO, "integer"),
      Pair.newPair(INT_TYPE_INFO, "short"),
      Pair.newPair(INT_TYPE_INFO, "long"),
      Pair.newPair(INT_TYPE_INFO, "byte"),
      Pair.newPair(DOUBLE_TYPE_INFO, "half_float"),
      Pair.newPair(DOUBLE_TYPE_INFO, "scaled_float")
  );

  @Test
  public void testTypeConverter() {
    final BaseEngineTypeInfoConverter converter;
    converter = new ElasticsearchWriterGenerator<>().createTypeInfoConverter();

    toBitSailTypePairs.forEach(pair -> {
      TypeInfo<?> typeInfo = converter.toTypeInfo(pair.getSecond());
      Assert.assertEquals(pair.getFirst().getClass(), typeInfo.getClass());
    });
  }
}
