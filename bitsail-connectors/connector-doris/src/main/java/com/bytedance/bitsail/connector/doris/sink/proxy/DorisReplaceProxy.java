/*
 * Copyright 2022 Bytedance Ltd. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bytedance.bitsail.connector.doris.sink.proxy;

import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.connector.doris.committer.DorisCommittable;
import com.bytedance.bitsail.connector.doris.config.DorisExecutionOptions;
import com.bytedance.bitsail.connector.doris.config.DorisOptions;
import com.bytedance.bitsail.connector.doris.error.DorisErrorCode;
import com.bytedance.bitsail.connector.doris.http.model.RespContent;
import com.bytedance.bitsail.connector.doris.sink.DorisWriterState;
import com.bytedance.bitsail.connector.doris.sink.label.LabelGenerator;
import com.bytedance.bitsail.connector.doris.sink.record.RecordStream;
import com.bytedance.bitsail.connector.doris.sink.streamload.DorisStreamLoad;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.bytedance.bitsail.connector.doris.sink.streamload.LoadStatus.PUBLISH_TIMEOUT;
import static com.bytedance.bitsail.connector.doris.sink.streamload.LoadStatus.SUCCESS;

public class DorisReplaceProxy extends AbstractDorisWriteModeProxy {
  private static final Logger LOG = LoggerFactory.getLogger(DorisReplaceProxy.class);
  private static final List<String> DORIS_SUCCESS_STATUS = new ArrayList<>(Arrays.asList(SUCCESS, PUBLISH_TIMEOUT));
  private RecordStream recordStream;
  private LabelGenerator labelGenerator;
  private DorisWriterState dorisWriterState;
  private AtomicInteger cacheRecordSize;
  private AtomicInteger cacheRecordCount;
  private volatile boolean loading = false;
  private final ArrayList<byte[]> cache = new ArrayList<>();
  private volatile Exception loadException = null;
  private int flushRecordCacheSize;
  private int flushRecordCacheCount;
  private byte[] lineDelimiter;
  private int intervalTime;
  private ScheduledExecutorService scheduler;
  private final int initialDelay = 1000;

  public DorisReplaceProxy(DorisExecutionOptions dorisExecutionOptions, DorisOptions dorisOptions) {
    this.dorisExecutionOptions = dorisExecutionOptions;
    this.dorisOptions = dorisOptions;
    this.labelGenerator = new LabelGenerator(dorisExecutionOptions.getLabelPrefix(), dorisExecutionOptions.isEnable2PC());
    this.recordStream = new RecordStream(dorisExecutionOptions.getBufferSize(), dorisExecutionOptions.getBufferCount());
    this.dorisStreamLoad = new DorisStreamLoad(dorisExecutionOptions, dorisOptions, labelGenerator, recordStream);
    this.dorisWriterState = new DorisWriterState(dorisExecutionOptions.getLabelPrefix());
    this.lineDelimiter = dorisOptions.getLineDelimiter().getBytes();
    this.intervalTime = dorisExecutionOptions.getCheckInterval();
    this.cacheRecordSize = new AtomicInteger();
    this.cacheRecordCount = new AtomicInteger();
    this.scheduler = Executors.newScheduledThreadPool(1,
        new BasicThreadFactory.Builder().namingPattern("Doris-replace-writer").daemon(true).build());
    scheduler.scheduleWithFixedDelay(this::checkDone, initialDelay, intervalTime, TimeUnit.MILLISECONDS);
  }

  @VisibleForTesting
  public DorisReplaceProxy() {
  }

  @Override
  public void write(String record) throws IOException {
    checkLoadException();
    byte[] bytes = record.getBytes(StandardCharsets.UTF_8);
    ArrayList<byte[]> tmpCache = null;
    if (cacheRecordCount.get() >= dorisExecutionOptions.getRecordCount() || cacheRecordSize.get() >= dorisExecutionOptions.getRecordSize()) {
      tmpCache = new ArrayList<>(cache);
      flushRecordCacheSize = cacheRecordSize.get();
      flushRecordCacheCount = cacheRecordCount.get();
      cache.clear();
      cacheRecordCount.set(0);
      cacheRecordSize.set(0);
    }
    cacheRecordSize.getAndAdd(bytes.length);
    cacheRecordCount.getAndIncrement();
    cache.add(bytes);

    if (Objects.nonNull(tmpCache)) {
      flush(tmpCache);
    }
  }

  private void flush(ArrayList<byte[]> flushCache) {
    if (!loading) {
      LOG.info("start load by cache full, recordCount {}, recordSize {}", flushRecordCacheCount, flushRecordCacheSize);
      try {
        startLoad(flushCache);
      } catch (Exception e) {
        LOG.error("start stream load failed.", e);
        loadException = e;
      }
    }
  }

  @Override
  public void flush(boolean endOfInput) throws IOException {
  }

  private synchronized void startLoad(List<byte[]> flushCache) throws IOException {
    this.dorisStreamLoad.startLoad(labelGenerator.generateLabel(), true);
    if (!flushCache.isEmpty()) {
      // add line delimiter
      ByteBuffer buf = ByteBuffer.allocate(flushRecordCacheSize + (flushCache.size() - 1) * lineDelimiter.length);
      for (int i = 0; i < flushCache.size(); i++) {
        if (i > 0) {
          buf.put(lineDelimiter);
        }
        buf.put(flushCache.get(i));
      }
      byte[] array = buf.array();
      dorisStreamLoad.writeRecord(buf.array());
      this.dorisStreamLoad.writeRecord(array);
    }
    this.loading = true;
  }

  @Override
  public List<DorisCommittable> prepareCommit() throws IOException {
    if (loading) {
      LOG.info("stop load by prepareCommit.");
      stopLoad();
      return ImmutableList.of(new DorisCommittable(dorisStreamLoad.getHostPort(), dorisOptions.getDatabaseName(), 0));
    }
    return Collections.emptyList();
  }

  private synchronized void stopLoad() throws IOException {
    this.loading = false;
    this.flushRecordCacheSize = 0;
    RespContent respContent = dorisStreamLoad.stopLoad();
    if (!DORIS_SUCCESS_STATUS.contains(respContent.getStatus())) {
      String errMsg = String.format("stream load error: %s, see more in %s", respContent.getMessage(), respContent.getErrorURL());
      LOG.warn(errMsg);
      throw new BitSailException(DorisErrorCode.LOAD_FAILED, errMsg);
    }
  }

  @Override
  public List<DorisWriterState> snapshotState(long checkpointId) {
    return Collections.singletonList(dorisWriterState);
  }

  private synchronized void checkDone() {
    LOG.info("start timer checker, interval {} ms", intervalTime);
    try {
      if (!loading) {
        LOG.info("not loading, skip timer checker");
        return;
      }
      if (dorisStreamLoad.getPendingLoadFuture() != null
          && !dorisStreamLoad.getPendingLoadFuture().isDone()) {
        LOG.info("stop load by timer checker");
        stopLoad();
      }
    } catch (Exception e) {
      LOG.error("stream load failed, thread exited:", e);
      loadException = e;
    }
  }

  private void checkLoadException() {
    if (loadException != null) {
      LOG.error("loading error.", loadException);
      throw new RuntimeException("error while loading data.", loadException);
    }
  }
}
