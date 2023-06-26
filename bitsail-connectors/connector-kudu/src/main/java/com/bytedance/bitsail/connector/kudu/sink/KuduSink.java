/*
 * Copyright 2022-2023 Bytedance Ltd. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bytedance.bitsail.connector.kudu.sink;

import com.bytedance.bitsail.base.connector.writer.v1.Sink;
import com.bytedance.bitsail.base.connector.writer.v1.Writer;
import com.bytedance.bitsail.base.connector.writer.v1.state.EmptyState;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.model.ColumnInfo;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.common.type.TypeInfoConverter;
import com.bytedance.bitsail.common.type.filemapping.FileMappingTypeInfoConverter;
import com.bytedance.bitsail.connector.kudu.core.KuduConstants;
import com.bytedance.bitsail.connector.kudu.core.KuduFactory;
import com.bytedance.bitsail.connector.kudu.option.KuduWriterOptions;
import com.bytedance.bitsail.connector.kudu.sink.schema.KuduSchemaAlignment;
import com.bytedance.bitsail.connector.kudu.util.KuduSchemaUtils;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Type;
import org.apache.kudu.client.KuduTable;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public class KuduSink<CommitT extends Serializable> implements Sink<Row, CommitT, EmptyState> {

  private BitSailConfiguration writerConf;

  private static final HashMap<String, Type> TYPE_MAPPINGS = new HashMap<>();

  static {
    TYPE_MAPPINGS.put("int8", Type.INT8);
    TYPE_MAPPINGS.put("int16", Type.INT16);
    TYPE_MAPPINGS.put("int32", Type.INT32);
    TYPE_MAPPINGS.put("int64", Type.INT64);
    TYPE_MAPPINGS.put("binary", Type.BINARY);
    TYPE_MAPPINGS.put("string", Type.STRING);
    TYPE_MAPPINGS.put("bool", Type.BOOL);
    TYPE_MAPPINGS.put("float", Type.FLOAT);
    TYPE_MAPPINGS.put("double", Type.DOUBLE);
    TYPE_MAPPINGS.put("unixtime_micros", Type.UNIXTIME_MICROS);
    TYPE_MAPPINGS.put("decimal", Type.DECIMAL);
    TYPE_MAPPINGS.put("varchar", Type.VARCHAR);
    TYPE_MAPPINGS.put("date", Type.DATE);
  }

  @Override
  public String getWriterName() {
    return KuduConstants.KUDU_CONNECTOR_NAME;
  }

  @Override
  public void configure(BitSailConfiguration commonConfiguration, BitSailConfiguration writerConfiguration) {
    this.writerConf = writerConfiguration;

    String tableName = writerConf.get(KuduWriterOptions.KUDU_TABLE_NAME);
    KuduFactory kuduFactory = KuduFactory.initWriterFactory(writerConf);
    KuduTable kuduTable = kuduFactory.getTable(tableName);

    List<ColumnInfo> columns = writerConf.get(KuduWriterOptions.COLUMNS);
    Boolean schemaAlign = writerConf.get(KuduWriterOptions.SCHEMA_ALIGN);
    if (schemaAlign) {
      List<ColumnSchema> targetSchema = columns.stream().map(column -> new ColumnSchema.ColumnSchemaBuilder(column.getName(),
          TYPE_MAPPINGS.get(column.getType().trim().toLowerCase()))
          .defaultValue(column.getDefaultValue())
          .comment(column.getComment())
          .build()).collect(Collectors.toList());
      KuduSchemaAlignment kuduSchemaAlignment = new KuduSchemaAlignment(kuduFactory.getClient(), kuduTable);
      kuduSchemaAlignment.align(kuduTable.getSchema().getColumns(), targetSchema);
    } else {
      KuduSchemaUtils.checkColumnsExist(kuduTable, columns);
    }

  }

  @Override
  public Writer<Row, CommitT, EmptyState> createWriter(Writer.Context<EmptyState> context) {
    return new KuduWriter<>(writerConf);
  }

  @Override
  public TypeInfoConverter createTypeInfoConverter() {
    return new FileMappingTypeInfoConverter(getWriterName());
  }
}
