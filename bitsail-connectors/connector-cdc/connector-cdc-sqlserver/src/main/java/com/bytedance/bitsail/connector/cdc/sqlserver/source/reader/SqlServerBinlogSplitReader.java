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

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.connector.cdc.source.reader.BinlogSplitReader;
import com.bytedance.bitsail.connector.cdc.source.split.BinlogSplit;
import com.bytedance.bitsail.connector.cdc.sqlserver.source.config.SqlServerConfig;
import com.bytedance.bitsail.connector.cdc.sqlserver.source.util.DebeziumUtils;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.debezium.config.Configuration;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.sqlserver.SqlServerConnection;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig;
import io.debezium.connector.sqlserver.SqlServerDatabaseSchema;
import io.debezium.connector.sqlserver.SqlServerErrorHandler;
import io.debezium.connector.sqlserver.SqlServerOffsetContext;
import io.debezium.connector.sqlserver.SqlServerStreamingChangeEventSource;
import io.debezium.connector.sqlserver.SqlServerTaskContext;
import io.debezium.connector.sqlserver.SqlServerTopicSelector;
import io.debezium.connector.sqlserver.SqlServerValueConverters;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;
import io.debezium.util.Clock;
import io.debezium.util.SchemaNameAdjuster;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SqlServerBinlogSplitReader implements BinlogSplitReader<SourceRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(SqlServerBinlogSplitReader.class);

  private boolean isRunning;

  private final SqlServerConfig sqlserverConfig;

  private final SchemaNameAdjuster schemaNameAdjuster;

  private ChangeEventQueue<DataChangeEvent> queue;

  private volatile SqlServerTaskContext taskContext;

  private volatile SqlServerConnection dataConnection;

  private volatile SqlServerConnection metadataConnection;

  private volatile ErrorHandler errorHandler;

  private final ExecutorService executorService;

  private SqlServerStreamingChangeEventSource dbzSource;

  private TopicSelector<TableId> topicSelector;

  private List<SourceRecord> batch;

  private Iterator<SourceRecord> recordIterator;

  private SqlServerConnectorConfig connectorConfig;

  private Map<String, ?> offset;

  private final int subtaskId;

  private volatile SqlServerDatabaseSchema schema;

  private final BitSailConfiguration jobConf;

  public SqlServerBinlogSplitReader(BitSailConfiguration jobConf,
                                    int subtaskId,
                                    long instantId) {
    this.jobConf = jobConf;
    this.sqlserverConfig = SqlServerConfig.fromBitSailConf(jobConf, instantId);
    this.schemaNameAdjuster = SchemaNameAdjuster.create();
    this.connectorConfig = sqlserverConfig.getDbzSqlServerConnectorConfig();
    this.subtaskId = subtaskId;
    this.offset = new HashMap<>();
    this.executorService = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat("sqlserver-binlog-reader-" + this.subtaskId).build());
    this.isRunning = false;
  }

  @Override
  public void readSplit(BinlogSplit split) {
    LOG.info("Read split: " + split.toString());
    this.topicSelector = SqlServerTopicSelector.defaultSelector(connectorConfig);
    final SqlServerValueConverters valueConverters = new SqlServerValueConverters(connectorConfig.getDecimalMode(),
        connectorConfig.getTemporalPrecisionMode(), connectorConfig.binaryHandlingMode());

    final Configuration dbzConfiguration = sqlserverConfig.getDbzConfiguration();
    this.dataConnection = new SqlServerConnection(connectorConfig.jdbcConfig(), Clock.system(),
        connectorConfig.getSourceTimestampMode(), valueConverters);
    this.metadataConnection = new SqlServerConnection(connectorConfig.jdbcConfig(), Clock.system(),
        connectorConfig.getSourceTimestampMode(), valueConverters);

    SqlServerOffsetContext offsetContext;
    try {
      dataConnection.execute("SELECT name FROM sys.databases;");
      offsetContext = DebeziumUtils.loadOffsetContext(connectorConfig, split, dataConnection);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    this.offset = offsetContext.getOffset();
    this.schema = new SqlServerDatabaseSchema(connectorConfig, valueConverters, topicSelector, schemaNameAdjuster);
    this.schema.initializeStorage();
    //schema.recover(offsetContext);
    taskContext = new SqlServerTaskContext(connectorConfig, schema);
    // Set up the task record queue ...
    this.queue = new ChangeEventQueue.Builder<DataChangeEvent>()
        .pollInterval(connectorConfig.getPollInterval())
        .maxBatchSize(connectorConfig.getMaxBatchSize())
        .maxQueueSize(connectorConfig.getMaxQueueSize())
        .maxQueueSizeInBytes(connectorConfig.getMaxQueueSizeInBytes())
        .loggingContextSupplier(() -> taskContext.configureLoggingContext("sqlserver-connector-task"))
        .build();

    this.errorHandler = new SqlServerErrorHandler(connectorConfig.getLogicalName(), queue);

    final SqlServerEventMetadataProvider metadataProvider = new SqlServerEventMetadataProvider();

    final EventDispatcher<TableId> dispatcher = new EventDispatcher<>(
        connectorConfig,
        topicSelector,
        schema,
        queue,
        connectorConfig.getTableFilters().dataCollectionFilter(),
        DataChangeEvent::new,
        metadataProvider,
        schemaNameAdjuster);

    this.dbzSource = new SqlServerStreamingChangeEventSource(
        connectorConfig,
        dataConnection,
        metadataConnection,
        dispatcher,
        errorHandler,
        Clock.system(),
        schema
    );

    try {
      dbzSource.init();
      this.isRunning = true;
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    executorService.submit(
        () -> {
          try {
            dbzSource.execute(
                new BinlogChangeEventSourceContext(),
                offsetContext
            );
          } catch (InterruptedException e) {
            isRunning = false;
            throw new RuntimeException(e);
          }
        }
    );
  }

  @Override
  public Map<String, String> getOffset() {
    Map<String, String> offsetToStore = new HashMap<>();
    this.offset.forEach((k, v) -> {
      LOG.info("offset key:{}, offset value: {}", k, v);
      if (v != null) {
        offsetToStore.put(k, v.toString());
      }
    });
    return offsetToStore;
  }

  @Override
  public void close() {
    LOG.info("Received close signal on SqlServerBinlogSplitReader");
    try {
      if (executorService != null) {
        executorService.shutdown();
      }
      if (this.dataConnection != null) {
        this.dataConnection.close();
      }
      if (this.metadataConnection != null) {
        this.metadataConnection.close();
      }
      isRunning = false;
    } catch (Exception e) {
      LOG.error("Failed to close SqlServerBinlogSplitReader, exist anyway.", e);
    }
  }

  @Override
  public SourceRecord poll() {
    return this.recordIterator.next();
  }

  @Override
  public boolean hasNext() {
    if (recordIterator != null && recordIterator.hasNext()) {
      return true;
    } else {
      return pollNextBatch();
    }
  }

  private boolean pollNextBatch() {
    if (isRunning) {
      try {
        List<DataChangeEvent> dbzRecords = queue.poll();
        if (dbzRecords.isEmpty()) {
          return false;
        }
        this.batch = new ArrayList<>();
        for (DataChangeEvent event : dbzRecords) {
          this.batch.add(event.getRecord());
        }
        this.recordIterator = this.batch.iterator();
        return true;
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    return false;
  }

  @Override
  public boolean isRunning() {
    return isRunning;
  }

  private class BinlogChangeEventSourceContext
      implements ChangeEventSource.ChangeEventSourceContext {
    @Override
    public boolean isRunning() {
      return isRunning;
    }
  }
}
