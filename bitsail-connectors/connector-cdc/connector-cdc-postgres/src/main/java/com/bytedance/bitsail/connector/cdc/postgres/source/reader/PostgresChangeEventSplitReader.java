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
package com.bytedance.bitsail.connector.cdc.postgres.source.reader;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.connector.cdc.jdbc.source.config.AbstractJdbcDebeziumConfig;
import com.bytedance.bitsail.connector.cdc.jdbc.source.reader.AbstractJdbcChangeEventSplitReader;
import com.bytedance.bitsail.connector.cdc.jdbc.source.streaming.AbstractSplitChangeEventStreamingTaskContext;
import com.bytedance.bitsail.connector.cdc.postgres.source.config.PostgresConfig;
import com.bytedance.bitsail.connector.cdc.postgres.source.streaming.PostgresChangeEventStreamingTaskContext;
import com.bytedance.bitsail.connector.cdc.source.split.BinlogSplit;
import io.debezium.relational.RelationalDatabaseConnectorConfig;

import java.io.IOException;
import java.sql.SQLException;

public class PostgresChangeEventSplitReader extends AbstractJdbcChangeEventSplitReader {
  public PostgresChangeEventSplitReader(BitSailConfiguration jobConf, int subtaskId) {
    super(jobConf, subtaskId);
  }

  @Override
  public AbstractJdbcDebeziumConfig getJdbcDebeziumConfig(BitSailConfiguration jobConf) {
    return new PostgresConfig(jobConf);
  }

  @Override
  public AbstractSplitChangeEventStreamingTaskContext getSplitReaderTaskContext(BinlogSplit split, RelationalDatabaseConnectorConfig connectorConfig) {
    return new PostgresChangeEventStreamingTaskContext(split, connectorConfig);
  }
}
