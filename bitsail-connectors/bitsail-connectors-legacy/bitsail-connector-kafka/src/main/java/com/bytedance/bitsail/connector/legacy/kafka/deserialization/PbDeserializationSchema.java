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

package com.bytedance.bitsail.connector.legacy.kafka.deserialization;

import com.bytedance.bitsail.batch.file.parser.PbBytesParser;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.flink.core.typeinfo.PrimitiveColumnTypeInfo;

import com.google.protobuf.Descriptors;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.List;

@Internal
public class PbDeserializationSchema implements DeserializationSchema<Row> {
  private static final long serialVersionUID = -2556547991095476394L;
  private final PbBytesParser parser;
  private final RowTypeInfo rowTypeInfo;
  private final int arity;

  public PbDeserializationSchema(BitSailConfiguration jobConf) throws Exception {
    this.parser = new PbBytesParser(jobConf);

    List<Descriptors.FieldDescriptor> fields = parser.getDescriptor().getFields();
    this.arity = fields.size();
    PrimitiveColumnTypeInfo<?>[] types = new PrimitiveColumnTypeInfo[arity];
    String[] fieldNames = new String[arity];
    for (int i = 0; i < arity; i++) {
      Descriptors.FieldDescriptor field = fields.get(i);
      types[i] = getColumnTypeInfo(field);
      fieldNames[i] = field.getJsonName();
    }
    this.rowTypeInfo = new RowTypeInfo(types, fieldNames);
  }

  private PrimitiveColumnTypeInfo<?> getColumnTypeInfo(Descriptors.FieldDescriptor field) {
    switch (field.getJavaType()) {
      case INT:
      case LONG:
        return PrimitiveColumnTypeInfo.LONG_COLUMN_TYPE_INFO;
      case FLOAT:
      case DOUBLE:
        return PrimitiveColumnTypeInfo.DOUBLE_COLUMN_TYPE_INFO;
      case BOOLEAN:
        return PrimitiveColumnTypeInfo.BOOL_COLUMN_TYPE_INFO;
      case BYTE_STRING:
        return PrimitiveColumnTypeInfo.BYTES_COLUMN_TYPE_INFO;
      case MESSAGE:
      case STRING:
      case ENUM:
      default:
        return PrimitiveColumnTypeInfo.STRING_COLUMN_TYPE_INFO;
    }
  }

  @Override
  public void open(DeserializationSchema.InitializationContext context) throws Exception {
  }

  @Override
  public Row deserialize(byte[] value) throws IOException {
    return this.parser.parse(new Row(arity), value, 0, value.length, null, rowTypeInfo);
  }

  @Override
  public void deserialize(byte[] value, Collector<Row> out) throws IOException {
    out.collect(deserialize(value));
  }

  @Override
  public boolean isEndOfStream(Row row) {
    return false;
  }

  @Override
  public TypeInformation<Row> getProducedType() {
    return rowTypeInfo;
  }
}
