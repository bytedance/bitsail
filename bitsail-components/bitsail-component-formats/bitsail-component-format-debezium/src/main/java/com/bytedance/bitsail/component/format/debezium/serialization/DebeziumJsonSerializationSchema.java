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

package com.bytedance.bitsail.component.format.debezium.serialization;

import com.bytedance.bitsail.base.format.SerializationSchema;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.common.typeinfo.RowTypeInfo;

public class DebeziumJsonSerializationSchema implements SerializationSchema<Row> {

  private final RowTypeInfo rowTypeInfo;
  private final int fieldNameIndex;

  public DebeziumJsonSerializationSchema(RowTypeInfo rowTypeInfo,
                                         String fieldName) {
    this.rowTypeInfo = rowTypeInfo;
    this.fieldNameIndex = rowTypeInfo.indexOf(fieldName);
  }

  @Override
  public byte[] serialize(Row element) {
    return element.getBinary(fieldNameIndex);
  }
}
