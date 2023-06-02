/*
 *       Copyright 2022-2023 Bytedance Ltd. and/or its affiliates.
 *
 *       Licensed under the Apache License, Version 2.0 (the "License");
 *       you may not use this file except in compliance with the License.
 *       You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *       Unless required by applicable law or agreed to in writing, software
 *       distributed under the License is distributed on an "AS IS" BASIS,
 *       WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *       See the License for the specific language governing permissions and
 *       limitations under the License.
 */

package com.bytedance.bitsail.component.format.debezium.deserialization;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.row.Row;

import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DebeziumJsonDeserializationSchemaTest {

  private DebeziumDeserializationSchema deserializationSchema;

  @Before
  public void before() {
    this.deserializationSchema = new DebeziumJsonDeserializationSchema(BitSailConfiguration.newDefault());
    this.deserializationSchema.open();
  }

  @Test
  public void test() {
    Row row = deserializationSchema.deserialize(mockSourceRecord());
    Assert.assertEquals(row.getArity(), DebeziumJsonDeserializationSchema.DEBEZIUM_JSON_ROW_TYPE.getFieldNames().length);
  }

  private static SourceRecord mockSourceRecord() {
    return new SourceRecord(Maps.newHashMap(),
        Maps.newHashMap(),
        StringUtils.EMPTY,
        null,
        null,
        null,
        null);
  }
}