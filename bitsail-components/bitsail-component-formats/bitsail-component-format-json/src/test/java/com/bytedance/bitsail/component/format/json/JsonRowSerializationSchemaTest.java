/*
 * Copyright 2022-2023 Bytedance Ltd. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.bytedance.bitsail.component.format.json;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.common.typeinfo.BasicArrayTypeInfo;
import com.bytedance.bitsail.common.typeinfo.RowTypeInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfos;
import com.bytedance.bitsail.component.format.json.JsonRowDeserializationSchema;
import com.bytedance.bitsail.component.format.json.JsonRowSerializationSchema;

import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;

public class JsonRowSerializationSchemaTest {
  @Test
  public void testNormalSerDes() {
    BitSailConfiguration jobConf = BitSailConfiguration.newDefault();
    TypeInfo<?>[] typeInfos = {
        TypeInfos.VOID_TYPE_INFO,
        TypeInfos.BOOLEAN_TYPE_INFO,
        TypeInfos.SHORT_TYPE_INFO,
        TypeInfos.INT_TYPE_INFO,
        TypeInfos.LONG_TYPE_INFO,
        TypeInfos.BIG_INTEGER_TYPE_INFO,
        TypeInfos.FLOAT_TYPE_INFO,
        TypeInfos.DOUBLE_TYPE_INFO,
        TypeInfos.BIG_DECIMAL_TYPE_INFO,
        TypeInfos.STRING_TYPE_INFO,
        BasicArrayTypeInfo.BINARY_TYPE_INFO};
    String[] fieldNames = {"void", "boolean", "short", "int", "long",
        "big_int", "float", "double", "big_decimal", "string", "binary"};

    Row expected = new Row(typeInfos.length);
    expected.setField(0, null);
    expected.setField(1, true);
    expected.setField(2, (short) 1);
    expected.setField(3, 2);
    expected.setField(4, Long.MAX_VALUE);
    expected.setField(5, new BigInteger("9223372036854775809")); // larger than max long
    expected.setField(6, 2.12f);
    expected.setField(7, 3.22d);
    expected.setField(8, new BigDecimal("3.141592653"));
    expected.setField(9, "Gary");
    expected.setField(10, "Gary".getBytes());

    JsonRowSerializationSchema serializationSchema = new JsonRowSerializationSchema(jobConf, new RowTypeInfo(fieldNames, typeInfos));
    JsonRowDeserializationSchema deserializationSchema = new JsonRowDeserializationSchema(jobConf, new RowTypeInfo(fieldNames, typeInfos));
    byte[] serialized = serializationSchema.serialize(expected);
    Row deserialized = deserializationSchema.deserialize(serialized);
    Assert.assertArrayEquals(expected.getFields(), deserialized.getFields());
  }
}
