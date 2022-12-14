/*
 * Copyright 2022-present, Bytedance Ltd.
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

package com.bytedance.bitsail.connector.base.source;

import com.bytedance.bitsail.base.connector.reader.v1.SourceReader;

import java.util.Collections;
import java.util.List;

public abstract class SimpleSourceReaderBase<T> implements SourceReader<T, SimpleSourceSplit> {

  @Override
  public void start() {
    //ignore
  }

  @Override
  public void addSplits(List<SimpleSourceSplit> splits) {
    //No splits need to add.
  }

  @Override
  public List<SimpleSourceSplit> snapshotState(long checkpointId) {
    return Collections.emptyList();
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) throws Exception {
    //Ignore
  }

  @Override
  public void close() throws Exception {
    //Ignore
  }
}
