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

package com.bytedance.bitsail.connector.legacy.streamingfile.common.filesystem.rollingpolicies;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.api.functions.sink.filesystem.Bucket;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;

import java.io.IOException;
import java.io.Serializable;

/**
 * The policy based on which a {@link Bucket} in the {@link StreamingFileSink}
 * rolls its currently open part file and opens a new one.
 */
@PublicEvolving
public interface RollingPolicy<IN> extends Serializable {

  /**
   * Determines if the in-progress part file for a bucket should roll based on its current state, e.g. its size.
   *
   * @param element the element being processed.
   * @return {@code True} if the part file should roll, {@code False} otherwise.
   */
  boolean shouldRollOnEvent(final PartFileInfo partFileState, IN element) throws IOException;

  /**
   * Determines if the in-progress part file for a bucket should roll based on a time condition.
   *
   * @param currentTime the current processing time.
   * @return {@code True} if the part file should roll, {@code False} otherwise.
   */
  boolean shouldRollOnProcessingTime(final PartFileInfo partFileState, final long currentTime) throws IOException;
}
