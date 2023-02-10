package com.bytedance.bitsail.connector.cdc.jdbc.source.streaming;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

@Getter
public class SplitChangeEventStreamingTaskController {
  public SplitChangeEventStreamingTaskContext splitReaderTaskContext;
  private ExecutorService executorService;
  public int subTaskId;
  private boolean isRunning;
  private static final Logger LOG = LoggerFactory.getLogger(SplitChangeEventStreamingTaskController.class);

  public SplitChangeEventStreamingTaskController(SplitChangeEventStreamingTaskContext splitReaderTaskContext, int subTaskId) {
    this.splitReaderTaskContext = splitReaderTaskContext;
    this.subTaskId = subTaskId;
    ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat(splitReaderTaskContext.threadNamePrefix() + this.subTaskId).build();
    this.executorService = Executors.newSingleThreadExecutor(threadFactory);
  }

  public void launchSplitReaderTask () throws InterruptedException {
    StreamingChangeEventSource dbzSource = splitReaderTaskContext.getStreamingChangeEventSource();
    dbzSource.init();
    this.isRunning = true;
    this.executorService.submit(() -> {
      try {
        dbzSource.execute(
                new BinlogChangeEventSourceContext(),
                splitReaderTaskContext.getOffsetContext());
      } catch (Exception e) {
        this.isRunning = false;
        LOG.error("Execute debezium binlog reader failed", e);
      }});
  }

  /**
   * close execution service
   */
  public void closeTask() {
    if (executorService != null) {
      executorService.shutdown();
    }
    isRunning = false;
  }

  private class BinlogChangeEventSourceContext
          implements ChangeEventSource.ChangeEventSourceContext {
    @Override
    public boolean isRunning() {
      return isRunning;
    }
  }
}
