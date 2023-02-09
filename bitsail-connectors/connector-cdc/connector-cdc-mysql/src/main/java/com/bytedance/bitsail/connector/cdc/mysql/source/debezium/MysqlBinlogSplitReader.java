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
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.connector.cdc.mysql.source.config.MysqlConfig;
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
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * Reader that actually execute the Debezium task.
 */
public class MysqlBinlogSplitReader implements BinlogSplitReader<Row> {

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

  public MysqlBinlogSplitReader(BitSailConfiguration jobConf, int subtaskId) {
    this.mysqlConfig = MysqlConfig.fromBitSailConf(jobConf);
    this.schemaNameAdjuster = SchemaNameAdjuster.create();
    // handle configuration
    this.connectorConfig = mysqlConfig.getDbzMySqlConnectorConfig();
    this.subtaskId = subtaskId;
    ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("mysql-binlog-reader-" + this.subtaskId).build();
    this.executorService = Executors.newSingleThreadExecutor(threadFactory);
    this.offset = new HashMap<>();
  }

  public void readSplit(BinlogSplit split) {
    this.split = split;
    this.offset = new HashMap<>();
    MySqlOffsetContext offsetContext = DebeziumHelper.loadOffsetContext(connectorConfig, split);

    this.topicSelector = MySqlTopicSelector.defaultSelector(connectorConfig);

    final MySqlValueConverters valueConverters = DebeziumHelper.getValueConverters(connectorConfig);

    this.queue = new ChangeEventQueue.Builder<DataChangeEvent>()
        .pollInterval(connectorConfig.getPollInterval())
        .maxBatchSize(connectorConfig.getMaxBatchSize())
        .maxQueueSize(connectorConfig.getMaxQueueSize())
        .maxQueueSizeInBytes(connectorConfig.getMaxQueueSizeInBytes())
        .loggingContextSupplier(() -> taskContext.configureLoggingContext("mysql-connector-task"))
        .buffering()
        .build();
    this.batch = new ArrayList<>();
    this.recordIterator = this.batch.iterator();
    this.errorHandler = new MySqlErrorHandler(connectorConfig.getLogicalName(), queue);

    final MySqlEventMetadataProvider metadataProvider = new MySqlEventMetadataProvider();

    final Configuration dbzConfiguration = mysqlConfig.getDbzConfiguration();

    this.connection = new MySqlConnection(
        new MySqlConnection.MySqlConnectionConfiguration(dbzConfiguration),
        connectorConfig.useCursorFetch() ? new MysqlBinaryProtocolFieldReader()
            : new MysqlTextProtocolFieldReader());

    //    Connection connection = DriverManager.getConnection(
    //        connectorConfig.getJdbcConfig().getHostname(), username, password);
    //    Statement statement = connection.createStatement();
    //    LOG.info("executing sql: {}", sql);

    try {
      connection.connect();
      connection.execute("SELECT version()");
    } catch (SQLException e) {
      throw new RuntimeException("Failed to connect", e);
    }

    DebeziumHelper.validateBinlogConfiguration(connectorConfig, connection);

    final boolean tableIdCaseInsensitive = connection.isTableIdCaseSensitive();

    this.schema = new MySqlDatabaseSchema(connectorConfig, valueConverters, topicSelector, schemaNameAdjuster, tableIdCaseInsensitive);
    this.taskContext = new MySqlTaskContext(connectorConfig, schema);

    this.binaryLogClient = this.taskContext.getBinaryLogClient();

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
        taskContext, // reuse binary log client
        new MySqlStreamingChangeEventSourceMetrics(
            taskContext, queue, metadataProvider)
    );
    this.isRunning = true;
    try {
      dbzSource.init();
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

  /**
   * return the binlog offset being processed
   */
  @Override
  public Map<String, String> getOffset() {
    Map<String, String> offsetToStore = new HashMap<>();
    this.offset.forEach((k, v) -> offsetToStore.put(k, v.toString()));
    return offsetToStore;
  }

  public void close() {
    try {
      if (this.connection != null) {
        this.connection.close();
      }
      if (this.binaryLogClient != null) {
        this.binaryLogClient.disconnect();
      }
      if (executorService != null) {
        executorService.shutdown();
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

  public boolean isCompleted() {
    return !isRunning;
  }

  public Row poll() {
    SourceRecord record = this.recordIterator.next();
    this.offset = record.sourceOffset();
    LOG.info("OFFSET:" + record.sourceOffset());
    LOG.info("poll one record {}", record.value());
    // TODO: Build BitSail row and return
    return null;
    //return record;
  }

  public boolean hasNext() throws InterruptedException {
    if (this.recordIterator.hasNext()) {
      return true;
    } else {
      return pollNextBatch();
    }
  }

  @SuppressWarnings("checkstyle:MagicNumber")
  private boolean pollNextBatch() throws InterruptedException {
    if (isRunning) {
      List<DataChangeEvent> dbzRecords = queue.poll();
      while (dbzRecords.isEmpty()) {
        //sleep 10s
        LOG.info("No record found, sleep for 5s in reader");
        TimeUnit.SECONDS.sleep(5);
        dbzRecords = queue.poll();
      }
      this.batch = new ArrayList<>();
      for (DataChangeEvent event : dbzRecords) {
        this.batch.add(event.getRecord());
      }
      this.recordIterator = this.batch.iterator();
      return true;
    }
    return false;
  }
}
