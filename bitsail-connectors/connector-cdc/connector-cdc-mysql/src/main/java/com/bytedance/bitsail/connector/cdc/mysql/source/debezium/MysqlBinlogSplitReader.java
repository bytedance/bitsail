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

package com.bytedance.bitsail.connector.cdc.mysql.source.debezium;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.connector.cdc.mysql.source.config.MysqlConfig;
import com.bytedance.bitsail.connector.cdc.mysql.source.schema.SchemaUtils;
import com.bytedance.bitsail.connector.cdc.mysql.source.schema.TableChangeConverter;
import com.bytedance.bitsail.connector.cdc.option.BinlogReaderOptions;
import com.bytedance.bitsail.connector.cdc.source.reader.BinlogSplitReader;
import com.bytedance.bitsail.connector.cdc.source.split.BinlogSplit;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.debezium.config.Configuration;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.mysql.MySqlConnection;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.MySqlDatabaseSchema;
import io.debezium.connector.mysql.MySqlErrorHandler;
import io.debezium.connector.mysql.MySqlOffsetContext;
import io.debezium.connector.mysql.MySqlStreamingChangeEventSource;
import io.debezium.connector.mysql.MySqlStreamingChangeEventSourceMetrics;
import io.debezium.connector.mysql.MySqlTaskContext;
import io.debezium.connector.mysql.MySqlTopicSelector;
import io.debezium.connector.mysql.MySqlValueConverters;
import io.debezium.connector.mysql.MysqlBinaryProtocolFieldReader;
import io.debezium.connector.mysql.MysqlTextProtocolFieldReader;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;
import io.debezium.schema.TopicSelector;
import io.debezium.util.Clock;
import io.debezium.util.SchemaNameAdjuster;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * Reader that actually execute the Debezium task.
 * This reader is stateless and will read binlog starting from the given begin offset.
 */
public class MysqlBinlogSplitReader implements BinlogSplitReader<SourceRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(MysqlBinlogSplitReader.class);

  private boolean isRunning;

  private final MysqlConfig mysqlConfig;

  private final SchemaNameAdjuster schemaNameAdjuster;

  private ChangeEventQueue<DataChangeEvent> queue;

  private EventDispatcher<TableId> dispatcher;

  private MySqlConnectorConfig connectorConfig;

  private volatile MySqlTaskContext taskContext;

  private volatile MySqlConnection connection;

  private volatile ErrorHandler errorHandler;

  private volatile MySqlDatabaseSchema schema;

  private final ExecutorService executorService;

  private MySqlStreamingChangeEventSource dbzSource;

  private BinaryLogClient binaryLogClient;

  private TopicSelector<TableId> topicSelector;

  private List<SourceRecord> batch;
  private Iterator<SourceRecord> recordIterator;

  private BinlogSplit split;

  private Map<String, ?> offset;

  private final int subtaskId;

  private final BitSailConfiguration jobConf;

  public MysqlBinlogSplitReader(BitSailConfiguration jobConf,
                                int subtaskId,
                                long instantId) {
    this.jobConf = jobConf;
    this.mysqlConfig = MysqlConfig.fromBitSailConf(jobConf, instantId);
    this.schemaNameAdjuster = SchemaNameAdjuster.create();
    // handle configuration
    this.connectorConfig = mysqlConfig.getDbzMySqlConnectorConfig();
    this.subtaskId = subtaskId;
    ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("mysql-binlog-reader-" + this.subtaskId).build();
    this.executorService = Executors.newSingleThreadExecutor(threadFactory);
    this.offset = new HashMap<>();
    this.isRunning = false;
  }

  public void readSplit(BinlogSplit split) {
    this.topicSelector = MySqlTopicSelector.defaultSelector(connectorConfig);

    final MySqlValueConverters valueConverters = DebeziumHelper.getValueConverters(connectorConfig);

    final MySqlEventMetadataProvider metadataProvider = new MySqlEventMetadataProvider();

    final Configuration dbzConfiguration = mysqlConfig.getDbzConfiguration();

    this.connection = new MySqlConnection(
        new MySqlConnection.MySqlConnectionConfiguration(dbzConfiguration),
        connectorConfig.useCursorFetch() ? new MysqlBinaryProtocolFieldReader()
            : new MysqlTextProtocolFieldReader());

    try {
      connection.connect();
      connection.execute("SELECT version()");
    } catch (SQLException e) {
      throw new RuntimeException("Failed to connect", e);
    }

    Map<TableId, TableChanges.TableChange> tableSchemas;
    try {
      if (split.getSchemas().isEmpty()) {
        tableSchemas = SchemaUtils.discoverCapturedTableSchemas(connection, connectorConfig);
        this.split = new BinlogSplit(split.uniqSplitId(), split.getBeginOffset(), split.getEndOffset(), TableChangeConverter.tableChangeToString(tableSchemas));
      } else {
        Map<String, String> rawSchema = split.getSchemas();
        tableSchemas = TableChangeConverter.stringToTableChange(rawSchema);
        this.split = split;
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    InMemoryDatabaseHistory.registerHistory(mysqlConfig.getDbzConfiguration().getString(InMemoryDatabaseHistory.DATABASE_HISTORY_INSTANCE_NAME),
        tableSchemas.values());

    DebeziumHelper.validateBinlogConfiguration(connectorConfig, connection);
    MySqlOffsetContext offsetContext = DebeziumHelper.loadOffsetContext(connectorConfig, split, connection);
    this.offset = offsetContext.getOffset();
    final boolean tableIdCaseInsensitive = connection.isTableIdCaseSensitive();
    this.schema = new MySqlDatabaseSchema(connectorConfig, valueConverters, topicSelector, schemaNameAdjuster, tableIdCaseInsensitive);
    schema.initializeStorage();
    schema.recover(offsetContext);
    for (TableId tableId : tableSchemas.keySet()) {
      LOG.debug("Schema for TableId " + tableId.toString() + ":" + schema.schemaFor(tableId).toString());
    }
    this.taskContext = new MySqlTaskContext(connectorConfig, schema);
    this.binaryLogClient = this.taskContext.getBinaryLogClient();
    this.queue = new ChangeEventQueue.Builder<DataChangeEvent>()
        .pollInterval(Duration.ofMillis(jobConf.get(BinlogReaderOptions.POLL_INTERVAL_MS)))
        .maxBatchSize(jobConf.get(BinlogReaderOptions.MAX_BATCH_SIZE))
        .maxQueueSize(jobConf.get(BinlogReaderOptions.MAX_QUEUE_SIZE))
        .loggingContextSupplier(() -> taskContext.configureLoggingContext("mysql-connector-task"))
        .buffering()
        .build();
    this.batch = new ArrayList<>();
    this.recordIterator = this.batch.iterator();
    this.errorHandler = new MySqlErrorHandler(connectorConfig.getLogicalName(), queue);

    this.dispatcher = new EventDispatcher<>(
        connectorConfig,
        topicSelector,
        schema,
        queue,
        connectorConfig.getTableFilters().dataCollectionFilter(),
        DataChangeEvent::new,
        metadataProvider,
        schemaNameAdjuster);

    this.dbzSource = new MySqlStreamingChangeEventSource(
        connectorConfig,
        connection,
        dispatcher,
        errorHandler,
        Clock.SYSTEM,
        taskContext,
        new MySqlStreamingChangeEventSourceMetrics(
            taskContext, queue, metadataProvider)
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
                offsetContext);
          } catch (Exception e) {
            isRunning = false;
            LOG.error("Execute debezium binlog reader failed", e);
          }
        });
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

  public void close() {
    LOG.info("Received close signal on MysqlBinlogSplitReader");
    try {
      if (executorService != null) {
        executorService.shutdown();
      }
      if (this.connection != null) {
        this.connection.close();
      }
      if (this.binaryLogClient != null) {
        this.binaryLogClient.disconnect();
      }
      isRunning = false;
    } catch (Exception e) {
      LOG.error("Failed to close MysqlBinlogSplitReader, exist anyway.", e);
    }
  }

  private class BinlogChangeEventSourceContext
      implements ChangeEventSource.ChangeEventSourceContext {
    @Override
    public boolean isRunning() {
      return isRunning;
    }
  }

  @Override
  public boolean isRunning() {
    return isRunning;
  }

  @Override
  public SourceRecord poll() {
    return recordIterator.next();
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
}
