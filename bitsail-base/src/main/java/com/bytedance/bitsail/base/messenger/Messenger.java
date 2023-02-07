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

package com.bytedance.bitsail.base.messenger;

import java.io.Closeable;
import java.io.Serializable;

/**
 * an interface for collecting statistics in job runtime
 * including two kinds of statistics:
 */
public interface Messenger extends Serializable, Closeable {
  /**
   * Open messenger for reporting statistics.
   */
  void open();

  /**
   * Report a succeeded record.
   */
  void addSuccessRecord(long byteSize);

  /**
   * Report a failed record.
   *
   * @param throwable Cause for which the record failed.
   */
  void addFailedRecord(Throwable throwable);

  /**
   * Update how many splits are finished when a job has multiple splits to process.
   */
  default void recordSplitProgress() {

  }

  /**
   * @return The number of succeeded records.
   */
  long getSuccessRecords();

  /**
   * @return The bytes number of succeeded records.
   */
  long getSuccessRecordBytes();

  /**
   * @return The number of failed records.
   */
  long getFailedRecords();

  /**
   * Commit collected statistics to somewhere when tasks finish.
   */
  default void commit(){

  }
}
