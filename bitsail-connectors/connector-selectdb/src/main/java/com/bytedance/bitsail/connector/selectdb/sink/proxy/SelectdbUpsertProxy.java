/*
 * Copyright 2022-2023 Bytedance Ltd. and/or its affiliates.
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

package com.bytedance.bitsail.connector.selectdb.sink.proxy;

import com.bytedance.bitsail.common.util.Preconditions;
import com.bytedance.bitsail.connector.selectdb.committer.SelectdbCommittable;
import com.bytedance.bitsail.connector.selectdb.config.SelectdbExecutionOptions;
import com.bytedance.bitsail.connector.selectdb.config.SelectdbOptions;
import com.bytedance.bitsail.connector.selectdb.copyinto.CopySQLBuilder;
import com.bytedance.bitsail.connector.selectdb.sink.SelectdbWriterState;
import com.bytedance.bitsail.connector.selectdb.sink.label.LabelGenerator;
import com.bytedance.bitsail.connector.selectdb.sink.uploadload.SelectdbUploadLoad;

import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class SelectdbUpsertProxy extends AbstractSelectdbWriteModeProxy {
  private static final Logger LOG = LoggerFactory.getLogger(SelectdbUpsertProxy.class);
  private static final long MAX_CACHE_SIZE = 1024 * 1024L;
  private final SelectdbExecutionOptions executionOptions;
  private final SelectdbOptions selectdbOptions;
  private long checkpointId = 0;
  private final AtomicInteger fileNum;
  private final byte[] lineDelimiter;
  private final LabelGenerator labelGenerator;
  private final SelectdbWriterState selectdbWriterState;
  private final ArrayList<byte[]> cache = new ArrayList<>();
  private int cacheSize = 0;
  private int cacheCnt = 0;
  private volatile boolean loading = false;

  public SelectdbUpsertProxy(SelectdbExecutionOptions executionOptions, SelectdbOptions selectdbOptions) {
    this.executionOptions = executionOptions;
    this.selectdbOptions = selectdbOptions;
    this.selectdbWriterState = new SelectdbWriterState(this.executionOptions.getLabelPrefix());
    this.labelGenerator = new LabelGenerator(executionOptions.getLabelPrefix());
    this.fileNum = new AtomicInteger();
    this.lineDelimiter = selectdbOptions.getLineDelimiter().getBytes(StandardCharsets.UTF_8);
    this.selectdbUploadLoad = new SelectdbUploadLoad(executionOptions, selectdbOptions);
  }

  @Override
  public void write(String record) throws IOException {
    byte[] bytes = record.getBytes(StandardCharsets.UTF_8);
    if (cacheSize > MAX_CACHE_SIZE) {
      if (!loading) {
        LOG.info("start load by cache full, cnt {}, size {}", cacheCnt, cacheSize);
        startLoad();
      }
      selectdbUploadLoad.writeRecord(bytes);
    } else {
      cacheSize += bytes.length;
      cacheCnt++;
      cache.add(bytes);
    }
  }

  @Override
  public void flush(boolean endOfInput) throws IOException {
  }

  @Override
  public List<SelectdbCommittable> prepareCommit() throws IOException {
    // disable exception checker before stop load.
    Preconditions.checkState(selectdbUploadLoad != null);
    if (!loading) {
      //No data was written during the entire checkpoint period
      LOG.info("start load by checkpoint, cnt {} size {} ", cacheCnt, cacheSize);
      startLoad();
    }
    LOG.info("stop load by checkpoint");
    stopLoad();
    CopySQLBuilder copySQLBuilder = new CopySQLBuilder(selectdbOptions, executionOptions, selectdbUploadLoad.getFileList());
    String copySql = copySQLBuilder.buildCopySQL();
    return ImmutableList.of(new SelectdbCommittable(selectdbUploadLoad.getHostPort(), selectdbOptions.getClusterName(), copySql));
  }

  @Override
  public List<SelectdbWriterState> snapshotState(long checkpointId) {
    Preconditions.checkState(selectdbUploadLoad != null);
    this.checkpointId = checkpointId + 1;

    LOG.info("clear the file list {}", selectdbUploadLoad.getFileList());
    this.fileNum.set(0);
    selectdbUploadLoad.clearFileList();
    return Collections.singletonList(selectdbWriterState);
  }

  private synchronized void startLoad() throws IOException {
    // If not started writing, make a streaming request
    this.selectdbUploadLoad.startLoad(labelGenerator.generateLabel(checkpointId, fileNum.getAndIncrement()));
    if (!cache.isEmpty()) {
      // add line delimiter
      ByteBuffer buf = ByteBuffer.allocate(cacheSize + (cache.size() - 1) * lineDelimiter.length);
      for (int i = 0; i < cache.size(); i++) {
        if (i > 0) {
          buf.put(lineDelimiter);
        }
        buf.put(cache.get(i));
      }
      this.selectdbUploadLoad.writeRecord(buf.array());
    }
    this.loading = true;
  }

  private synchronized void stopLoad() throws IOException {
    this.loading = false;
    this.selectdbUploadLoad.stopLoad();
    this.cacheSize = 0;
    this.cacheCnt = 0;
    this.cache.clear();
  }

}
