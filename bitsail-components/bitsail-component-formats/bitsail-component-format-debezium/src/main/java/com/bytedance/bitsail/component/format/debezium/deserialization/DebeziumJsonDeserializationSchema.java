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

import com.bytedance.bitsail.base.extension.SupportProducedType;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.common.typeinfo.BasicArrayTypeInfo;
import com.bytedance.bitsail.common.typeinfo.RowTypeInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfos;
import com.bytedance.bitsail.component.format.debezium.option.DebeziumWriterOptions;

import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.ConverterConfig;
import org.apache.kafka.connect.storage.ConverterType;

import java.util.HashMap;
import java.util.Objects;

public class DebeziumJsonDeserializationSchema implements DebeziumDeserializationSchema, SupportProducedType {
  public static final String NAME = "debezium_json";

  public static final String TOPIC_NAME = "topic";
  public static final String KEY_NAME = "key";
  public static final String VALUE_NAME = "value";
  public static final String TIMESTAMP_NAME = "timestamp";

  public static final RowTypeInfo DEBEZIUM_JSON_ROW_TYPE =
      new RowTypeInfo(
          new String[] {TOPIC_NAME, KEY_NAME, VALUE_NAME, TIMESTAMP_NAME},
          new TypeInfo<?>[] {
              TypeInfos.STRING_TYPE_INFO, BasicArrayTypeInfo.BINARY_TYPE_INFO, BasicArrayTypeInfo.BINARY_TYPE_INFO, TypeInfos.LONG_TYPE_INFO
          });

  private final BitSailConfiguration jobConf;
  private transient JsonConverter jsonConverter;

  public DebeziumJsonDeserializationSchema(BitSailConfiguration jobConf) {
    this.jobConf = jobConf;
  }

  @Override
  public RowTypeInfo getProducedType() {
    return DEBEZIUM_JSON_ROW_TYPE;
  }

  @Override
  public void open() {
    this.jsonConverter = new JsonConverter();
    boolean includeSchema = jobConf.get(DebeziumWriterOptions.DEBEZIUM_JSON_INCLUDE_SCHEMA);
    final HashMap<String, Object> configs = new HashMap<>();
    configs.put(ConverterConfig.TYPE_CONFIG, ConverterType.VALUE.getName());
    configs.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, includeSchema);
    jsonConverter.configure(configs);
  }

  @SuppressWarnings("checkstyle:MagicNumber")
  @Override
  public Row deserialize(SourceRecord sourceRecord) {
    byte[] key = jsonConverter.fromConnectData(sourceRecord.topic(), sourceRecord.keySchema(), sourceRecord.key());
    byte[] value = jsonConverter.fromConnectData(sourceRecord.topic(), sourceRecord.valueSchema(), sourceRecord.value());
    Object[] values = new Object[DEBEZIUM_JSON_ROW_TYPE.getFieldNames().length];
    values[0] = sourceRecord.topic();
    values[1] = Objects.isNull(key) ? null : key;
    values[2] = value;
    values[3] = sourceRecord.timestamp();
    return new Row(values);
  }

  @Override
  public boolean isEndOfStream(Row nextElement) {
    return false;
  }
}
