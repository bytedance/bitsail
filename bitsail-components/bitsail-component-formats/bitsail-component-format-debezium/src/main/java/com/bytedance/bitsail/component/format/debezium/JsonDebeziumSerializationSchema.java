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

package com.bytedance.bitsail.component.format.debezium;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.component.format.debezium.option.DebeziumWriterOptions;

import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.ConverterConfig;
import org.apache.kafka.connect.storage.ConverterType;

import java.util.HashMap;

public class JsonDebeziumSerializationSchema implements DebeziumSerializationSchema {
  private static final long serialVersionUID = 1L;

  private transient JsonConverter jsonConverter;

  public JsonDebeziumSerializationSchema(BitSailConfiguration jobConf) {
    this.jsonConverter = new JsonConverter();
    boolean includeSchema = jobConf.get(DebeziumWriterOptions.DEBEZIUM_JSON_INCLUDE_SCHEMA);
    final HashMap<String, Object> configs = new HashMap<>();
    configs.put(ConverterConfig.TYPE_CONFIG, ConverterType.VALUE.getName());
    configs.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, includeSchema);
    jsonConverter.configure(configs);
  }

  @Override
  public byte[] serialize(SourceRecord sourceRecord) {
    return jsonConverter.fromConnectData(sourceRecord.topic(), sourceRecord.valueSchema(), sourceRecord.value());
  }
}
