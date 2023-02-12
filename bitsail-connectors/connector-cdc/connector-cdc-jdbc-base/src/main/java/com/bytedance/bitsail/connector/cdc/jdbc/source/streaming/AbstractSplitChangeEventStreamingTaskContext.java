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
package com.bytedance.bitsail.connector.cdc.jdbc.source.streaming;

import com.bytedance.bitsail.connector.cdc.source.split.BinlogSplit;
import io.debezium.config.Configuration;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.RelationalDatabaseSchema;
import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;
import io.debezium.util.SchemaNameAdjuster;
import lombok.Getter;
import org.apache.kafka.connect.source.SourceTask;

import java.sql.SQLException;

@Getter
public abstract class AbstractSplitChangeEventStreamingTaskContext {

  /**
   * split info
   */
  public BinlogSplit split;

  /**
   * OffsetContext includes context and operations on offset
   */
  public OffsetContext offsetContext;

  /**
   * EventDispatcher is responsible to recognize event and emit to target ChangeEventQueue
   */
  public EventDispatcher eventDispatcher;

  /**
   * StreamingChangeEventSource is responsible to handle different change event, and deliver to EventDispatcher
   */
  public StreamingChangeEventSource streamingChangeEventSource;

  /**
   * Implementations return names for Kafka topics (data and meta-data).
   */
  public TopicSelector<TableId> topicSelector;

  /**
   * Adjuster to convert names of databases schema (e.g. fieldName) to valid Avro name
   */
  public SchemaNameAdjuster schemaNameAdjuster;

  /**
   * ErrorHandler is used when StreamingChangeEventSource has problems when read change event from database
   */
  public volatile ErrorHandler errorHandler;

  /**
   * RelationalDatabaseSchema is schema define of database
   */
  public volatile RelationalDatabaseSchema schema;

  /**
   * JdbcValueConverters is used to convert different type of values
   */
  public JdbcValueConverters valueConverters;

  /**
   * EventMetadataProvider is used to get metadata (e.g. tableName, tableId) from change event
   */
  public EventMetadataProvider metadataProvider;

  /**
   * Custom debezium configuration to start debezium task
   */
  public Configuration dbzConfiguration;

  /**
   * Contains contextual information and objects scoped to the lifecycle of Debezium's {@link SourceTask} implementations
   */
  public volatile CdcSourceTaskContext dbzTaskContext;

  /**
   * all related config of database
   */
  public RelationalDatabaseConnectorConfig connectorConfig;

  /**
   * Message queue to receive Change Event from event dispatcher
   */
  public ChangeEventQueue<DataChangeEvent> queue;

  /**
   * jdbc connection instance
   */
  public JdbcConnection jdbcConnection;

  public AbstractSplitChangeEventStreamingTaskContext(BinlogSplit split, RelationalDatabaseConnectorConfig connectorConfig) {
    this.split = split;
    this.connectorConfig = connectorConfig;
    this.jdbcConnection = tryEstablishedConnection(connectorConfig);
    this.offsetContext = buildOffsetContext();
    this.topicSelector = buildTopicSelector();
    this.valueConverters = buildValueConverters(connectorConfig);
    this.schema = buildRelationalDatabaseSchema(connectorConfig);
    this.errorHandler = buildErrorHandler(connectorConfig, queue);
    this.schemaNameAdjuster = SchemaNameAdjuster.create();
    this.dbzTaskContext = buildCdcSourceTaskContext(connectorConfig, schema);
    this.metadataProvider = buildEventMetadataProvider();
  }

  protected abstract ErrorHandler buildErrorHandler(RelationalDatabaseConnectorConfig connectorConfig, ChangeEventQueue<DataChangeEvent> queue);

  public void attachStreamingToQueue(ChangeEventQueue<DataChangeEvent> queue) {
    this.eventDispatcher = new EventDispatcher<>(
            connectorConfig,
            topicSelector,
            schema,
            queue,
            connectorConfig.getTableFilters().dataCollectionFilter(),
            DataChangeEvent::new,
            metadataProvider,
            schemaNameAdjuster);

    this.streamingChangeEventSource = buildStreamingChangeEventSource();
  }

  public void closeContextResources() throws SQLException {
    this.closeStreamingChangeEventSource();
    if (this.jdbcConnection.isConnected()) {
      this.jdbcConnection.close();
    }
  }

  public abstract void testConnectionAndValidBinlogConfiguration();

  /**
   * established a connection with jdbc database using the given config
   * @param connectorConfig
   * @return JdbcConnection
   */
  public abstract JdbcConnection tryEstablishedConnection(RelationalDatabaseConnectorConfig connectorConfig);

  /**
   * Give the thread name prefix of streaming task
   * @return
   */
  public abstract String threadNamePrefix();

  /**
   * tru to build offset context to implement all operation on offset
   * @return OffsetContext
   */
  public abstract OffsetContext buildOffsetContext();

  /**
   * Implementations return names for Kafka topics (data and meta-data).
   * @return TopicSelector
   */
  public abstract TopicSelector buildTopicSelector();

  /**
   * StreamingChangeEventSource is used to reader change event (e.g mysql binlog) from database, and emit by EventDispatcher
   * @return
   */
  public abstract StreamingChangeEventSource buildStreamingChangeEventSource();


  /**
   * close change evennt streaming task
   * @return
   */
  public abstract void closeStreamingChangeEventSource();

  /**
   * build RelationalDatabaseSchema base on config
   * @return RelationalDatabaseSchema
   */
  public abstract RelationalDatabaseSchema buildRelationalDatabaseSchema(RelationalDatabaseConnectorConfig connectorConfig);

  public abstract CdcSourceTaskContext buildCdcSourceTaskContext(RelationalDatabaseConnectorConfig connectorConfig, RelationalDatabaseSchema schema);

  public abstract EventMetadataProvider buildEventMetadataProvider();

  public abstract JdbcValueConverters buildValueConverters(RelationalDatabaseConnectorConfig connectorConfig);
}
