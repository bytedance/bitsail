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
package com.bytedance.bitsail.connector.cdc.postgres.source.streaming;

import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.connector.cdc.error.BinlogReaderErrorCode;
import com.bytedance.bitsail.connector.cdc.jdbc.source.streaming.AbstractSplitChangeEventStreamingTaskContext;
import com.bytedance.bitsail.connector.cdc.postgres.source.constant.PostgresConstant;
import com.bytedance.bitsail.connector.cdc.source.offset.BinlogOffset;
import com.bytedance.bitsail.connector.cdc.source.split.BinlogSplit;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.PostgresErrorHandler;
import io.debezium.connector.postgresql.PostgresEventMetadataProvider;
import io.debezium.connector.postgresql.PostgresOffsetContext;
import io.debezium.connector.postgresql.PostgresSchema;
import io.debezium.connector.postgresql.PostgresStreamingChangeEventSource;
import io.debezium.connector.postgresql.PostgresTaskContext;
import io.debezium.connector.postgresql.PostgresTopicSelector;
import io.debezium.connector.postgresql.PostgresValueConverter;
import io.debezium.connector.postgresql.TypeRegistry;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.ReplicationConnection;
import io.debezium.connector.postgresql.snapshot.NeverSnapshotter;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.RelationalDatabaseSchema;
import io.debezium.schema.TopicSelector;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.sql.SQLException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

public class PostgresChangeEventStreamingTaskContext extends AbstractSplitChangeEventStreamingTaskContext {
  private static final Logger LOG = LoggerFactory.getLogger(PostgresChangeEventStreamingTaskContext.class);
  private ReplicationConnection replicationConnection;

  public PostgresChangeEventStreamingTaskContext (BinlogSplit split, RelationalDatabaseConnectorConfig connectorConfig) {
    super(split, connectorConfig);
    this.replicationConnection = createReplicationConnection((PostgresTaskContext)dbzTaskContext, false, 3,
            Duration.ofMillis(Duration.ofSeconds(10).toMillis()));
  }

  @Override
  protected ErrorHandler buildErrorHandler(RelationalDatabaseConnectorConfig connectorConfig, ChangeEventQueue<DataChangeEvent> queue) {
    return new PostgresErrorHandler(connectorConfig.getLogicalName(), queue);
  }

  @Override
  public JdbcConnection tryEstablishedConnection(RelationalDatabaseConnectorConfig connectorConfig) {
    PostgresConnection connection = new PostgresConnection(connectorConfig.getJdbcConfig());
    return new PostgresConnection(connectorConfig.getJdbcConfig(), new TypeRegistry(connection));
  }

  @Override
  public String threadNamePrefix() {
    return "postgres-connector-task";
  }

  @Override
  public OffsetContext buildOffsetContext() {
    final PostgresOffsetContext offsetContext;

    switch (split.getBeginOffset().getOffsetType()) {
      case SPECIFIED:
        Map<String, ?> offset = buildPostgresOffsetProperties(split.getBeginOffset());
        offsetContext = new PostgresOffsetContext.Loader((PostgresConnectorConfig) connectorConfig).load(offset);
        break;
      //TODO: Add other offset context
      case LATEST:
        offsetContext = PostgresOffsetContext.initialContext((PostgresConnectorConfig) connectorConfig,
              (PostgresConnection) jdbcConnection, Clock.SYSTEM);
        break;
      default:
        throw new BitSailException(BinlogReaderErrorCode.UNSUPPORTED_ERROR,
                String.format("the begin binlog type %s is not supported", split.getBeginOffset().getOffsetType()));
      }
      return offsetContext;
  }

  public Map<String, String> buildPostgresOffsetProperties(BinlogOffset offset) {
    Map<String, String> offsetProperties = new HashMap<>();
    offsetProperties.put(PostgresConstant.LSN, offset.getProps().get(PostgresConstant.LSN));
    offsetProperties.put(PostgresConstant.TS_USEC, offset.getProps().get(PostgresConstant.TS_USEC));
    return offsetProperties;
  }

  @Override
  public TopicSelector buildTopicSelector() {
    return PostgresTopicSelector.create((PostgresConnectorConfig) connectorConfig);
  }

  @Override
  public StreamingChangeEventSource buildStreamingChangeEventSource() {
    return new PostgresStreamingChangeEventSource(
            (PostgresConnectorConfig) connectorConfig,
            new NeverSnapshotter(),
            (PostgresConnection)jdbcConnection,
            eventDispatcher,
            errorHandler,
            Clock.SYSTEM,
            (PostgresSchema) schema,
            (PostgresTaskContext) dbzTaskContext,
            this.replicationConnection);
  }

  @Override
  public void closeContextResources() {
    try {
      if (this.replicationConnection != null && this.replicationConnection.isConnected()) {
        this.replicationConnection.close();
      }
    } catch (Exception e) {
      LOG.error("Failed to close replication connection of pg streaming task context: {}", e.getMessage());
    }
  }

  @Override
  public void testConnectionAndValidBinlogConfiguration() {
    try {
      String testSql = "SELECT version()";
      jdbcConnection.connect();
      jdbcConnection.execute(testSql);
      LOG.info("Success to connect postgres!");
    } catch (SQLException e) {
      throw new RuntimeException("Failed to connect", e);
    }
  }

  public ReplicationConnection createReplicationConnection(PostgresTaskContext taskContext, boolean doSnapshot,
                                                           int maxRetries, Duration retryDelay)
          throws RuntimeException {
    final Metronome metronome = Metronome.parker(retryDelay, Clock.SYSTEM);
    short retryCount = 0;
    ReplicationConnection replicationConnection = null;
    while (retryCount <= maxRetries) {
      try {
        return taskContext.createReplicationConnection(doSnapshot);
      }
      catch (SQLException ex) {
        retryCount++;
        if (retryCount > maxRetries) {
          LOG.error("Too many errors connecting to server. All {} retries failed.", maxRetries);
          throw new RuntimeException(ex);
        }

        LOG.warn("Error connecting to server; will attempt retry {} of {} after {} " +
                "seconds. Exception message: {}", retryCount, maxRetries, retryDelay.getSeconds(), ex.getMessage());
        try {
          metronome.pause();
        }
        catch (InterruptedException e) {
          LOG.warn("Connection retry sleep interrupted by exception: " + e);
          Thread.currentThread().interrupt();
        }
      }
    }
    return replicationConnection;
  }

  @Override
  public void closeStreamingChangeEventSource() {
    LOG.info("Closing pg streaming task");
  }

  @Override
  public RelationalDatabaseSchema buildRelationalDatabaseSchema(RelationalDatabaseConnectorConfig connectorConfig) {
    return new PostgresSchema((PostgresConnectorConfig)connectorConfig,
            new TypeRegistry((PostgresConnection) jdbcConnection),
            topicSelector,
            (PostgresValueConverter) valueConverters);
  }

  @Override
  public CdcSourceTaskContext buildCdcSourceTaskContext(RelationalDatabaseConnectorConfig connectorConfig, RelationalDatabaseSchema schema) {
    return new PostgresTaskContext((PostgresConnectorConfig) connectorConfig, (PostgresSchema) schema, topicSelector);
  }

  @Override
  public EventMetadataProvider buildEventMetadataProvider() {
    return new PostgresEventMetadataProvider();
  }

  @Override
  public JdbcValueConverters buildValueConverters(RelationalDatabaseConnectorConfig connectorConfig) {
    return PostgresValueConverter.of(
            (PostgresConnectorConfig) connectorConfig,
            Charset.defaultCharset(),
            new TypeRegistry((PostgresConnection) jdbcConnection));
  }
}
