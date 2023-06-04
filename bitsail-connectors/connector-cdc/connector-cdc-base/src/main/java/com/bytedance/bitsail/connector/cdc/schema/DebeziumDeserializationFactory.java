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

package com.bytedance.bitsail.connector.cdc.schema;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.type.TypeInfoConverter;
import com.bytedance.bitsail.component.format.debezium.deserialization.DebeziumDeserializationSchema;
import com.bytedance.bitsail.component.format.debezium.deserialization.DebeziumJsonDeserializationSchema;
import com.bytedance.bitsail.connector.cdc.option.BinlogReaderOptions;

public class DebeziumDeserializationFactory {

  public static DebeziumDeserializationSchema getDebeziumDeserializationSchema(BitSailConfiguration jobConf, TypeInfoConverter typeInfoConverter) {
    String format = jobConf.get(BinlogReaderOptions.FORMAT_TYPE);

    if (DebeziumJsonDeserializationSchema.NAME.equalsIgnoreCase(format)) {
      return new DebeziumJsonDeserializationSchema(jobConf);
    }

    throw new UnsupportedOperationException();
  }
}
