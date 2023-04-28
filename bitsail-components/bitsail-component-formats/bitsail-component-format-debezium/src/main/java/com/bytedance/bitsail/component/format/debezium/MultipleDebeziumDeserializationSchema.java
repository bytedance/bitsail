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

package com.bytedance.bitsail.component.format.debezium;

import com.bytedance.bitsail.base.extension.SupportProducedType;
import com.bytedance.bitsail.common.catalog.table.TableId;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.row.MultipleTableRow;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.common.typeinfo.RowTypeInfo;
import com.bytedance.bitsail.component.format.debezium.option.DebeziumWriterOptions;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.ConverterConfig;
import org.apache.kafka.connect.storage.ConverterType;

import java.util.HashMap;

import static io.debezium.data.Envelope.FieldName.SOURCE;

public class MultipleDebeziumDeserializationSchema implements DebeziumDeserializationSchema, SupportProducedType {

  private transient JsonConverter jsonConverter;
  private BitSailConfiguration jobConf;

  public MultipleDebeziumDeserializationSchema(BitSailConfiguration jobConf) {
    this.jobConf = jobConf;
  }

  public void open() {
    this.jsonConverter = new JsonConverter();
    boolean includeSchema = jobConf.get(DebeziumWriterOptions.DEBEZIUM_JSON_INCLUDE_SCHEMA);
    final HashMap<String, Object> configs = new HashMap<>();
    configs.put(ConverterConfig.TYPE_CONFIG, ConverterType.VALUE.getName());
    configs.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, includeSchema);
    jsonConverter.configure(configs);
  }

  @Override
  public RowTypeInfo getProducedType() {
    return MultipleTableRow.MULTIPLE_TABLE_ROW_TYPE_INFO;
  }

  @Override
  public Row deserialize(SourceRecord sourceRecord) {
    Struct valueStruct = (Struct) sourceRecord.value();
    TableId tableId = TableId
        .of(valueStruct.getStruct(SOURCE).getString("db"), valueStruct.getStruct(SOURCE).getString("table"));
    byte[] connectData = jsonConverter.fromConnectData(sourceRecord.topic(), sourceRecord.valueSchema(), sourceRecord.value());
    MultipleTableRow multipleTableRow = MultipleTableRow
        .of(tableId.toString(),
            sourceRecord.topic(),
            new String(connectData),
            StringUtils.EMPTY,
            StringUtils.EMPTY);

    return multipleTableRow.asRow();
  }

  @Override
  public boolean isEndOfStream(Row nextElement) {
    return false;
  }
}
