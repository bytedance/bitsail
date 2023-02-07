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

package com.bytedance.bitsail.connector.doris.sink.proxy;

import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.connector.doris.committer.DorisCommittable;
import com.bytedance.bitsail.connector.doris.config.DorisExecutionOptions;
import com.bytedance.bitsail.connector.doris.config.DorisOptions;
import com.bytedance.bitsail.connector.doris.error.DorisErrorCode;
import com.bytedance.bitsail.connector.doris.http.model.RespContent;
import com.bytedance.bitsail.connector.doris.rest.RestService;
import com.bytedance.bitsail.connector.doris.sink.DorisWriterState;
import com.bytedance.bitsail.connector.doris.sink.label.LabelGenerator;
import com.bytedance.bitsail.connector.doris.sink.record.RecordStream;
import com.bytedance.bitsail.connector.doris.sink.streamload.DorisStreamLoad;

import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.bytedance.bitsail.connector.doris.sink.streamload.LoadStatus.PUBLISH_TIMEOUT;
import static com.bytedance.bitsail.connector.doris.sink.streamload.LoadStatus.SUCCESS;

public class DorisUpsertProxy extends AbstractDorisWriteModeProxy {
  private static final Logger LOG = LoggerFactory.getLogger(DorisUpsertProxy.class);
  private static final List<String> DORIS_SUCCESS_STATUS = new ArrayList<>(Arrays.asList(SUCCESS, PUBLISH_TIMEOUT));
  private final DorisExecutionOptions executionOptions;
  private final DorisOptions dorisOptions;
  private long lastCheckpointId = 0;
  private final String labelPrefix;
  private final LabelGenerator labelGenerator;
  private final DorisWriterState dorisWriterState;
  private final RecordStream recordStream;

  public DorisUpsertProxy(DorisExecutionOptions dorisExecutionOptions, DorisOptions dorisOptions) {
    this.executionOptions = dorisExecutionOptions;
    this.dorisOptions = dorisOptions;
    this.dorisWriterState = new DorisWriterState(executionOptions.getLabelPrefix());
    this.labelPrefix = executionOptions.getLabelPrefix();
    this.labelGenerator = new LabelGenerator(labelPrefix, dorisExecutionOptions.isEnable2PC());
    this.recordStream = new RecordStream(executionOptions.getBufferSize(), executionOptions.getBufferCount());
    init();
  }

  public void init() {
    this.dorisStreamLoad = new DorisStreamLoad(executionOptions, dorisOptions, labelGenerator, recordStream);
    try {
      if (executionOptions.isEnable2PC()) {
        dorisStreamLoad.abortPreCommit(labelPrefix, lastCheckpointId);
      }
      dorisStreamLoad.startLoad(labelGenerator.generateLabel(lastCheckpointId + 1));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void write(String record) throws IOException {
    dorisStreamLoad.writeRecord(record.getBytes(StandardCharsets.UTF_8));
  }

  @Override
  public void flush(boolean endOfInput) throws IOException {
  }

  @Override
  public List<DorisCommittable> prepareCommit() throws IOException {
    RespContent respContent = dorisStreamLoad.stopLoad();
    if (!DORIS_SUCCESS_STATUS.contains(respContent.getStatus())) {
      String errMsg = String.format("stream load error: %s, see more in %s", respContent.getMessage(), respContent.getErrorURL());
      LOG.warn(errMsg);
      throw new BitSailException(DorisErrorCode.LOAD_FAILED, errMsg);
    }
    if (!executionOptions.isEnable2PC()) {
      return Collections.emptyList();
    }

    long txnId = respContent.getTxnId();
    return ImmutableList.of(new DorisCommittable(dorisStreamLoad.getHostPort(), dorisOptions.getDatabaseName(), txnId));
  }

  @Override
  public List<DorisWriterState> snapshotState(long checkpointId) {
    lastCheckpointId = checkpointId;
    //Dynamically refresh be Node
    dorisStreamLoad.setHostPort(RestService.getAvailableHost());
    try {
      dorisStreamLoad.startLoad(labelGenerator.generateLabel(checkpointId + 1));
    } catch (IOException e) {
      LOG.warn("Failed to start load. checkpointId={}", checkpointId, e);
      throw new RuntimeException(e);
    }
    return Collections.singletonList(dorisWriterState);
  }

}
