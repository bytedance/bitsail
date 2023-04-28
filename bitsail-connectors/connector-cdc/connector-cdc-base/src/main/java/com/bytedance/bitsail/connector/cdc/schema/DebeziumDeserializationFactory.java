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
import com.bytedance.bitsail.common.model.ColumnInfo;
import com.bytedance.bitsail.common.option.ReaderOptions;
import com.bytedance.bitsail.common.type.TypeInfoConverter;
import com.bytedance.bitsail.common.typeinfo.RowTypeInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfoUtils;
import com.bytedance.bitsail.component.format.debezium.DebeziumDeserializationSchema;
import com.bytedance.bitsail.component.format.debezium.DebeziumJsonDeserializationSchema;
import com.bytedance.bitsail.component.format.debezium.DebeziumRowDeserializationSchema;
import com.bytedance.bitsail.component.format.debezium.MultipleDebeziumDeserializationSchema;
import com.bytedance.bitsail.connector.cdc.option.BinlogReaderOptions;

import java.util.List;

public class DebeziumDeserializationFactory {

  public static DebeziumDeserializationSchema getDebeziumDeserializationSchema(BitSailConfiguration jobConf, TypeInfoConverter typeInfoConverter) {
    Boolean multiple = jobConf.get(ReaderOptions.BaseReaderOptions.MULTIPLE_READER_ENABLED);
    if (multiple) {
      return new MultipleDebeziumDeserializationSchema(jobConf);
    }
    String format = jobConf.get(BinlogReaderOptions.FORMAT);
    if (DebeziumJsonDeserializationSchema.NAME.equalsIgnoreCase(format)) {
      return new DebeziumJsonDeserializationSchema(jobConf);
    }
    List<ColumnInfo> columnInfos = jobConf.get(ReaderOptions.BaseReaderOptions.COLUMNS);
    RowTypeInfo rowTypeInfo = TypeInfoUtils.getRowTypeInfo(typeInfoConverter, columnInfos);
    return new DebeziumRowDeserializationSchema(jobConf, rowTypeInfo);

  }
}
