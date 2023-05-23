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

package com.bytedance.bitsail.connector.cdc.sqlserver.source.reader;

import io.debezium.connector.sqlserver.SourceInfo;
import io.debezium.data.Envelope;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.schema.DataCollectionId;
import io.debezium.util.Collect;
import org.apache.kafka.connect.data.Struct;

import java.time.Instant;
import java.util.Map;

/**
 * Copied from debezium project.
 */
class SqlServerEventMetadataProvider implements EventMetadataProvider {

  @Override
  public Instant getEventTimestamp(DataCollectionId source, OffsetContext offset, Object key, Struct value) {
    if (value == null) {
      return null;
    }
    final Struct sourceInfo = value.getStruct(Envelope.FieldName.SOURCE);
    if (source == null) {
      return null;
    }
    final Long timestamp = sourceInfo.getInt64(SourceInfo.TIMESTAMP_KEY);
    return timestamp == null ? null : Instant.ofEpochMilli(timestamp);
  }

  @Override
  public Map<String, String> getEventSourcePosition(DataCollectionId source, OffsetContext offset, Object key, Struct value) {
    if (value == null) {
      return null;
    }
    final Struct sourceInfo = value.getStruct(Envelope.FieldName.SOURCE);
    if (source == null) {
      return null;
    }
    return Collect.hashMapOf(
        SourceInfo.COMMIT_LSN_KEY, sourceInfo.getString(SourceInfo.COMMIT_LSN_KEY),
        SourceInfo.CHANGE_LSN_KEY, sourceInfo.getString(SourceInfo.CHANGE_LSN_KEY));
  }

  @Override
  public String getTransactionId(DataCollectionId source, OffsetContext offset, Object key, Struct value) {
    if (value == null) {
      return null;
    }
    final Struct sourceInfo = value.getStruct(Envelope.FieldName.SOURCE);
    if (source == null) {
      return null;
    }
    return sourceInfo.getString(SourceInfo.COMMIT_LSN_KEY);
  }
}
