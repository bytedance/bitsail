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

package com.bytedance.bitsail.connector.selectdb.sink;

import com.bytedance.bitsail.base.connector.writer.v1.Writer;
import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.connector.selectdb.committer.SelectdbCommittable;
import com.bytedance.bitsail.connector.selectdb.config.SelectdbExecutionOptions;
import com.bytedance.bitsail.connector.selectdb.config.SelectdbOptions;
import com.bytedance.bitsail.connector.selectdb.error.SelectdbErrorCode;
import com.bytedance.bitsail.connector.selectdb.serialize.SelectdbRowSerializer;
import com.bytedance.bitsail.connector.selectdb.sink.proxy.AbstractSelectdbWriteModeProxy;
import com.bytedance.bitsail.connector.selectdb.sink.proxy.SelectdbUpsertProxy;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class SelectdbWriter<InputT extends Row> implements Writer<InputT, SelectdbCommittable, SelectdbWriterState> {
  private static final Logger LOG = LoggerFactory.getLogger(SelectdbWriter.class);
  protected BitSailConfiguration writerConfiguration;
  protected SelectdbRowSerializer serializer;
  protected AbstractSelectdbWriteModeProxy writeModeProxy;

  public SelectdbWriter(BitSailConfiguration writerConfiguration, SelectdbOptions selectdbOptions, SelectdbExecutionOptions selectdbExecutionOptions) {
    this.writerConfiguration = writerConfiguration;
    switch (selectdbExecutionOptions.getWriterMode()) {
      case STREAMING_UPSERT:
      case BATCH_UPSERT:
        this.writeModeProxy = new SelectdbUpsertProxy(selectdbExecutionOptions, selectdbOptions);
        break;
      default:
        throw new BitSailException(SelectdbErrorCode.PROXY_INIT_FAILED, "Write mode is not valid");
    }
    this.serializer = new SelectdbRowSerializer(selectdbOptions.getColumnInfos(), selectdbOptions.getLoadDataFormat(), selectdbOptions.getFieldDelimiter(),
        selectdbExecutionOptions.getEnableDelete());
  }

  @VisibleForTesting
  public SelectdbWriter() {
  }

  @Override
  public void write(InputT in) throws IOException {
    String record;
    record = this.serializer.serialize(in);
    if (StringUtils.isEmpty(record)) {
      return;
    }
    writeModeProxy.write(record);
  }

  @Override
  public void flush(boolean endOfInput) throws IOException {
    writeModeProxy.flush(endOfInput);
  }

  @Override
  public List<SelectdbCommittable> prepareCommit() throws IOException {
    return writeModeProxy.prepareCommit();
  }

  @Override
  public List<SelectdbWriterState> snapshotState(long checkpointId) throws IOException {
    return writeModeProxy.snapshotState(checkpointId);
  }
}

