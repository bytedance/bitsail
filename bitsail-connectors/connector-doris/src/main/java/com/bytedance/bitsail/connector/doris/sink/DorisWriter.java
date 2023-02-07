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

package com.bytedance.bitsail.connector.doris.sink;

import com.bytedance.bitsail.base.connector.writer.v1.Writer;
import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.connector.doris.committer.DorisCommittable;
import com.bytedance.bitsail.connector.doris.config.DorisExecutionOptions;
import com.bytedance.bitsail.connector.doris.config.DorisOptions;
import com.bytedance.bitsail.connector.doris.error.DorisErrorCode;
import com.bytedance.bitsail.connector.doris.serialize.DorisRowSerializer;
import com.bytedance.bitsail.connector.doris.sink.proxy.AbstractDorisWriteModeProxy;
import com.bytedance.bitsail.connector.doris.sink.proxy.DorisReplaceProxy;
import com.bytedance.bitsail.connector.doris.sink.proxy.DorisUpsertProxy;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class DorisWriter<InputT extends Row> implements Writer<InputT, DorisCommittable, DorisWriterState> {
  private static final Logger LOG = LoggerFactory.getLogger(DorisWriter.class);
  protected BitSailConfiguration writerConfiguration;
  protected DorisRowSerializer serializer;
  protected AbstractDorisWriteModeProxy writeModeProxy;

  public DorisWriter(BitSailConfiguration writerConfiguration, DorisOptions dorisOptions, DorisExecutionOptions dorisExecutionOptions) {
    this.writerConfiguration = writerConfiguration;

    switch (dorisExecutionOptions.getWriterMode()) {
      case STREAMING_UPSERT:
      case BATCH_UPSERT:
        writeModeProxy = new DorisUpsertProxy(dorisExecutionOptions, dorisOptions);
        break;
      case BATCH_REPLACE:
        if (!dorisExecutionOptions.isBatch()) {
          throw new BitSailException(DorisErrorCode.PROXY_INIT_FAILED, "Replace mode is only supported in batch");
        }
        writeModeProxy = new DorisReplaceProxy(dorisExecutionOptions, dorisOptions);
        break;
      default:
        throw new BitSailException(DorisErrorCode.PROXY_INIT_FAILED, "Write mode is not valid");
    }
    this.serializer = new DorisRowSerializer(dorisOptions.getColumnInfos(), dorisOptions.getLoadDataFormat(), dorisOptions.getFieldDelimiter(),
        dorisExecutionOptions.getEnableDelete());
  }

  @VisibleForTesting
  public DorisWriter() {
  }

  @Override
  public void write(InputT in) throws IOException {
    String dorisRecord;
    dorisRecord = this.serializer.serialize(in);
    if (StringUtils.isEmpty(dorisRecord)) {
      return;
    }
    writeModeProxy.write(dorisRecord);
  }

  @Override
  public void flush(boolean endOfInput) throws IOException {
    writeModeProxy.flush(endOfInput);
  }

  @Override
  public List<DorisCommittable> prepareCommit() throws IOException {
    return writeModeProxy.prepareCommit();
  }

  @Override
  public List<DorisWriterState> snapshotState(long checkpointId) throws IOException {
    return writeModeProxy.snapshotState(checkpointId);
  }
}

