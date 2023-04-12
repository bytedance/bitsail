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

package com.bytedance.bitsail.base.connector.reader.v1;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;

public interface SourceSplitCoordinator<SplitT extends SourceSplit, StateT> extends Serializable, AutoCloseable {

  void start();

  /**
   * Add a new source reader with the given subtask ID.
   */
  void addReader(int subtaskId);

  /**
   * Add splits back to the split enumerator. This will only happen when a SourceReader fails and there are splits
   * assigned to it after the last successful checkpoint.
   */
  void addSplitsBack(List<SplitT> splits, int subtaskId);

  /**
   * Handles the request for a split. This method is called when the reader with the given subtask id calls the
   * SourceReader.Context.sendSplitRequest() method.
   */
  void handleSplitRequest(int subtaskId, @Nullable String requesterHostname);

  default void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
  }

  StateT snapshotState(long checkpoint) throws Exception;

  default void notifyCheckpointComplete(long checkpointId) throws Exception {
  }

  void close();

  interface Context<SplitT extends SourceSplit, StateT> {

    boolean isRestored();

    /**
     * Return the state to the split coordinator, for the exactly-once.
     */
    StateT getRestoreState();

    /**
     * Return total parallelism of the source reader.
     */
    int totalParallelism();

    /**
     * When Source reader started, it will be registered itself to coordinator.
     */
    Set<Integer> registeredReaders();

    /**
     * Assign splits to reader.
     */
    void assignSplit(int subtaskId, List<SplitT> splits);

    /**
     * Mainly use in boundedness situation, represents there will no more split will send to source reader.
     */
    void signalNoMoreSplits(int subtask);

    /**
     * If split coordinator have any event want to send source reader, use this method.
     * Like send Pause event to Source Reader in CDC2.0.
     */
    void sendEventToSourceReader(int subtaskId, SourceEvent event);

    /**
     * Schedule to run the callable and handler, often used in un-boundedness mode.
     */
    <T> void runAsync(Callable<T> callable,
                      BiConsumer<T, Throwable> handler,
                      int initialDelay,
                      long interval);

    /**
     * Just run callable and handler once, often used in boundedness mode.
     */
    <T> void runAsyncOnce(Callable<T> callable,
                          BiConsumer<T, Throwable> handler);
  }
}
