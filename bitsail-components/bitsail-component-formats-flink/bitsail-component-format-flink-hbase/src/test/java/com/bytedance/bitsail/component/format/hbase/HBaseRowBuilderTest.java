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

package com.bytedance.bitsail.component.format.hbase;


import com.bytedance.bitsail.common.column.Column;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigInteger;

public class HBaseRowBuilderTest {
  private static final String ENCODING = "UTF-8";

  @Test
  public void testBasicTypeConversion() {
    HBaseRowBuilder builder = new HBaseRowBuilder();
    TypeInformation<Long> longType = TypeInformation.of(Long.class);
    Column longColumn = builder.createColumn(longType, ENCODING, Bytes.toBytesBinary("100"));
    Assert.assertEquals(longColumn.getRawData(), BigInteger.valueOf(100L));

    TypeInformation<Boolean> boolType = TypeInformation.of(Boolean.class);
    Column boolBitColumn = builder.createColumn(boolType, ENCODING, Bytes.toBytesBinary("1"));
    Column boolStrColumn = builder.createColumn(boolType, ENCODING, Bytes.toBytesBinary("true"));
    Assert.assertEquals(boolBitColumn.getRawData(), true);
    Assert.assertEquals(boolStrColumn.getRawData(), true);

    TypeInformation<String> strType = TypeInformation.of(String.class);
    Column strColumn = builder.createColumn(strType, ENCODING, Bytes.toBytesBinary("hello world"));
    Assert.assertEquals(strColumn.getRawData(), "hello world");

    TypeInformation<Double> doubleType = TypeInformation.of(Double.class);
    Column doubleColumn = builder.createColumn(doubleType, ENCODING, Bytes.toBytesBinary("1.6161"));
    Assert.assertEquals(doubleColumn.getRawData(), "1.6161");
  }
}
