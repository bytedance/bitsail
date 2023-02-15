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
package com.bytedance.bitsail.connector.cdc.jdbc.source.reader;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.connector.cdc.jdbc.source.config.AbstractJdbcDebeziumConfig;
import com.bytedance.bitsail.connector.cdc.jdbc.source.streaming.AbstractSplitChangeEventStreamingTaskContext;
import com.bytedance.bitsail.connector.cdc.jdbc.source.streaming.SplitChangeEventStreamingTaskController;
import com.bytedance.bitsail.connector.cdc.source.reader.BinlogSplitReader;
import com.bytedance.bitsail.connector.cdc.source.split.BinlogSplit;
import com.bytedance.bitsail.common.row.Row;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import lombok.Getter;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Getter
public abstract class AbstractJdbcChangeEventSplitReader implements BinlogSplitReader<Row> {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractJdbcChangeEventSplitReader.class);

  protected final AbstractJdbcDebeziumConfig jdbcDebeziumConfig;

  protected ChangeEventQueue<DataChangeEvent> queue;

  protected RelationalDatabaseConnectorConfig connectorConfig;

  protected List<SourceRecord> batch;

  protected Iterator<SourceRecord> recordIterator;

  protected Map<String, ?> offset;

  protected SplitChangeEventStreamingTaskController splitChangeEventStreamingTaskController;

  protected AbstractSplitChangeEventStreamingTaskContext splitChangeEventStreamingTaskContext;

  private final int subtaskId;

  public AbstractJdbcChangeEventSplitReader(BitSailConfiguration jobConf, int subtaskId) {
    jdbcDebeziumConfig = getJdbcDebeziumConfig(jobConf);
    connectorConfig = jdbcDebeziumConfig.getDbzJdbcConnectorConfig();
    this.subtaskId = subtaskId;
    this.offset = new HashMap<>();
  }

  public abstract AbstractJdbcDebeziumConfig getJdbcDebeziumConfig(BitSailConfiguration jobConf);

  public abstract AbstractSplitChangeEventStreamingTaskContext getSplitReaderTaskContext(BinlogSplit split, RelationalDatabaseConnectorConfig connectorConfig);

  public void inititialzeSplitReader(BinlogSplit split) {
    splitChangeEventStreamingTaskContext = getSplitReaderTaskContext(split, connectorConfig);
    this.offset = new HashMap<>();
    this.queue = new ChangeEventQueue.Builder<DataChangeEvent>()
            .pollInterval(connectorConfig.getPollInterval())
            .maxBatchSize(connectorConfig.getMaxBatchSize())
            .maxQueueSize(connectorConfig.getMaxQueueSize())
            .maxQueueSizeInBytes(connectorConfig.getMaxQueueSizeInBytes())
            .loggingContextSupplier(() -> splitChangeEventStreamingTaskContext.getDbzTaskContext()
                    .configureLoggingContext(splitChangeEventStreamingTaskContext.threadNamePrefix()))
            .buffering()
            .build();
    this.batch = new ArrayList<>();
    this.recordIterator = this.batch.iterator();
    splitChangeEventStreamingTaskContext.attachStreamingToQueue(this.queue);
    splitChangeEventStreamingTaskController = new SplitChangeEventStreamingTaskController(splitChangeEventStreamingTaskContext, this.subtaskId);
  }

  /**
   * Try to start streaming task to drain change event into target queue
   * @param split
   * @throws IOException
   * @throws InterruptedException
   */
  @Override
  public void readSplit(BinlogSplit split) throws IOException, InterruptedException {
    inititialzeSplitReader(split);
    splitChangeEventStreamingTaskContext.testConnectionAndValidBinlogConfiguration();
    splitChangeEventStreamingTaskController.launchSplitReaderTask();
  }

  /**
   * get the binlog offset being processed
   * @return
   */
  @Override
  public Map<String, String> getOffset() {
    Map<String, String> offsetToStore = new HashMap<>();
    this.offset.forEach((k, v) -> offsetToStore.put(k, v.toString()));
    return offsetToStore;
  }

  /**
   * close task and resources
   */
  @Override
  public void close() {
    try {
      splitChangeEventStreamingTaskController.closeTask();
    } catch (Exception e) {
      LOG.error("Failed to close change event streaming task: {}", e.getMessage());
    }

    try {
      splitChangeEventStreamingTaskContext.closeContextResources();
    } catch (Exception e) {
      LOG.error("Failed to close resources of streaming task context: {}", e.getMessage());
    }
  }

  @Override
  public Row poll() {
    SourceRecord record = this.recordIterator.next();
    this.offset = record.sourceOffset();
    LOG.info("OFFSET:" + record.sourceOffset());
    LOG.info("poll one record {}", record.value());
    // TODO: Build BitSail row and return
    return null;
  }

  /**
   * To judge whether current split has next record
   * @return
   * @throws Exception
   */
  @Override
  public boolean hasNext() {
    if (this.recordIterator.hasNext()) {
      return true;
    } else {
      return pollNextBatch();
    }
  }

  @Override
  public boolean isCompleted() {
    return !splitChangeEventStreamingTaskController.isRunning();
  }

  private boolean pollNextBatch() {
    if (splitChangeEventStreamingTaskController.isRunning()) {
      try {
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
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      return true;
    }
    return false;
  }
}
