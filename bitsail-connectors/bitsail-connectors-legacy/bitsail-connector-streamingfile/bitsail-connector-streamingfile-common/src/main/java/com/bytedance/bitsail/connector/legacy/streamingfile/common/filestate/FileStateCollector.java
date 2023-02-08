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

package com.bytedance.bitsail.connector.legacy.streamingfile.common.filestate;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.LinkedHashMap;

/**
 * Created 2020/3/11.
 */
public interface FileStateCollector extends Serializable, Closeable {
  /**
   * open resource
   */
  void open();

  /**
   * Collect dump file state from state backend.
   *
   * @param nextSuccessFileTimestamp current checked file timestamp.
   * @return matched file state.
   * @throws IOException throw when global status from hadoop filesystem.
   */
  FileState getFileState(long nextSuccessFileTimestamp) throws IOException;


  /**
   * Set partition info file's state field modify time as update time.
   *
   * @param taskId        task id
   * @param partitionInfo partition info
   */
  void updateFileState(int taskId, LinkedHashMap<String, String> partitionInfo);
}
