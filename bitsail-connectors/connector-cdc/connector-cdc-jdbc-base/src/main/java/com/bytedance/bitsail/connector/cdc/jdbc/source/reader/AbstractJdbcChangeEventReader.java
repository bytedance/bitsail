package com.bytedance.bitsail.connector.cdc.jdbc.source.reader;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.connector.cdc.jdbc.source.config.AbstractJdbcDebeziumConfig;
import com.bytedance.bitsail.connector.cdc.jdbc.source.streaming.SplitChangeEventStreamingTaskContext;
import com.bytedance.bitsail.connector.cdc.jdbc.source.streaming.SplitChangeEventStreamingTaskController;
import com.bytedance.bitsail.connector.cdc.source.reader.BinlogSplitReader;
import com.bytedance.bitsail.connector.cdc.source.split.BinlogSplit;
import com.bytedance.bitsail.common.row.Row;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
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

public abstract class AbstractJdbcChangeEventReader implements BinlogSplitReader<Row> {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractJdbcChangeEventReader.class);

  private final AbstractJdbcDebeziumConfig jdbcDebeziumConfig;

  private ChangeEventQueue<DataChangeEvent> queue;

  private RelationalDatabaseConnectorConfig connectorConfig;

  private List<SourceRecord> batch;

  private Iterator<SourceRecord> recordIterator;

  private CdcSourceTaskContext taskContext;

  private Map<String, ?> offset;

  private SplitChangeEventStreamingTaskController splitChangeEventStreamingTaskController;

  private SplitChangeEventStreamingTaskContext splitChangeEventStreamingTaskContext;

  private final int subtaskId;

  public AbstractJdbcChangeEventReader(BitSailConfiguration jobConf, int subtaskId) {
    jdbcDebeziumConfig = getJdbcDebeziumConfig(jobConf);
    connectorConfig = jdbcDebeziumConfig.getConnectorConfig();
    this.subtaskId = subtaskId;
    this.offset = new HashMap<>();
  }

  public AbstractJdbcDebeziumConfig getJdbcDebeziumConfig(BitSailConfiguration jobConf) {
    return AbstractJdbcDebeziumConfig.fromBitSailConf(jobConf);
  }

  public abstract SplitChangeEventStreamingTaskContext getSplitReaderTaskContext();

  public abstract void testConnectionAndValidBinlogConfiguration(RelationalDatabaseConnectorConfig connectorConfig) throws IOException;

  public void inititialzeSplitReader(BinlogSplit split) {
    splitChangeEventStreamingTaskContext = getSplitReaderTaskContext();
    this.offset = new HashMap<>();
    this.queue = new ChangeEventQueue.Builder<DataChangeEvent>()
            .pollInterval(connectorConfig.getPollInterval())
            .maxBatchSize(connectorConfig.getMaxBatchSize())
            .maxQueueSize(connectorConfig.getMaxQueueSize())
            .maxQueueSizeInBytes(connectorConfig.getMaxQueueSizeInBytes())
            .loggingContextSupplier(() -> taskContext.configureLoggingContext(splitChangeEventStreamingTaskContext.threadNamePrefix()))
            .buffering()
            .build();
    this.batch = new ArrayList<>();
    this.recordIterator = this.batch.iterator();
    splitChangeEventStreamingTaskContext.initializeSplitReaderTaskContext(connectorConfig, this.queue);
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
  public boolean hasNext() throws Exception {
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

  private boolean pollNextBatch() throws InterruptedException {
    if (splitChangeEventStreamingTaskController.isRunning()) {
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
