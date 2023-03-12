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

import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.common.typeinfo.BasicArrayTypeInfo;
import com.bytedance.bitsail.common.typeinfo.RowTypeInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfos;
import com.bytedance.bitsail.component.format.json.RowToJsonConverter;

import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;

public class RowToJsonConverterTest {

  @Test
  public void testRowToJsonString() {
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
    RowToJsonConverter converter = new RowToJsonConverter(new RowTypeInfo(fieldNames, typeInfos));

    Row row = new Row(typeInfos.length);
    row.setField(0, null);
    row.setField(1, true);
    row.setField(2, (short) 1);
    row.setField(3, 2);
    row.setField(4, Long.MAX_VALUE);
    row.setField(5, new BigInteger("9223372036854775809")); // larger than max long
    row.setField(6, 2.12f);
    row.setField(7, 3.22d);
    row.setField(8, new BigDecimal("3.141592653"));
    row.setField(9, "Gary");
    row.setField(10, "Gary".getBytes());

    String expected = "{\"void\":null,\"boolean\":true,\"short\":1,\"int\":2,\"long\":9223372036854775807,\"big_int\":9223372036854775809,\"float\":2.12," +
        "\"double\":3.22,\"big_decimal\":3.141592653,\"string\":\"Gary\",\"binary\":\"R2FyeQ==\"}";
    Assert.assertEquals(expected, converter.convert(row).toString());
  }
}
