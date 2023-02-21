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

import com.bytedance.bitsail.connector.selectdb.committer.SelectdbCommittable;
import com.bytedance.bitsail.connector.selectdb.config.SelectdbExecutionOptions;
import com.bytedance.bitsail.connector.selectdb.config.SelectdbOptions;
import com.bytedance.bitsail.connector.selectdb.sink.SelectdbWriterState;
import com.bytedance.bitsail.connector.selectdb.sink.uploadload.SelectdbUploadLoad;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

public abstract class AbstractSelectdbWriteModeProxy implements Serializable {
  protected SelectdbExecutionOptions selectdbExecutionOptions;
  protected SelectdbOptions selectdbOptions;
  protected SelectdbUploadLoad selectdbUploadLoad;

  public abstract void write(String record) throws IOException;

  public abstract void flush(boolean endOfInput) throws IOException;

  public abstract List<SelectdbCommittable> prepareCommit() throws IOException;

  public abstract List<SelectdbWriterState> snapshotState(long checkpointId);
}
