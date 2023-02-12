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
  public AbstractSplitChangeEventStreamingTaskContext splitReaderTaskContext;
  private ExecutorService executorService;
  public int subTaskId;
  private boolean isRunning;
  private static final Logger LOG = LoggerFactory.getLogger(SplitChangeEventStreamingTaskController.class);

  public SplitChangeEventStreamingTaskController(AbstractSplitChangeEventStreamingTaskContext splitReaderTaskContext, int subTaskId) {
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
