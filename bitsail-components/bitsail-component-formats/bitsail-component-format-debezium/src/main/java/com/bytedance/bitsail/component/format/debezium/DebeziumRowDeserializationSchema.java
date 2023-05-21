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

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.common.typeinfo.RowTypeInfo;

import org.apache.kafka.connect.source.SourceRecord;

public class DebeziumRowDeserializationSchema implements DebeziumDeserializationSchema {

  private final BitSailConfiguration jobConf;

  private final RowTypeInfo rowTypeInfo;

  public DebeziumRowDeserializationSchema(BitSailConfiguration jobConf, RowTypeInfo rowTypeInfo) {
    this.jobConf = jobConf;
    this.rowTypeInfo = rowTypeInfo;
  }

  @Override
  public void open() {

  }

  @Override
  public Row deserialize(SourceRecord sourceRecord) {
    //TODO
    return null;
  }

  @Override
  public boolean isEndOfStream(Row nextElement) {
    return false;
  }

  @Override
  public RowTypeInfo getProducedType() {
    return rowTypeInfo;
  }
}
