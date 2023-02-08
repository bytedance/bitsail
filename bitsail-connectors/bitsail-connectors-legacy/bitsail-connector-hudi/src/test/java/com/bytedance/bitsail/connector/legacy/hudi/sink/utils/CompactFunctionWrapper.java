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

package com.bytedance.bitsail.connector.legacy.hudi.sink.utils;

import com.bytedance.bitsail.connector.legacy.hudi.compact.CompactFunction;
import com.bytedance.bitsail.connector.legacy.hudi.compact.CompactionCommitEvent;
import com.bytedance.bitsail.connector.legacy.hudi.compact.CompactionCommitSink;
import com.bytedance.bitsail.connector.legacy.hudi.compact.CompactionPlanEvent;
import com.bytedance.bitsail.connector.legacy.hudi.compact.CompactionPlanOperator;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.coordination.MockOperatorCoordinatorContext;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.util.Collector;
import org.apache.hudi.avro.model.HoodieCompactionPlan;

import java.util.ArrayList;
import java.util.List;

public class CompactFunctionWrapper {
  private final Configuration conf;

  private final IOManager ioManager;
  private final StreamingRuntimeContext runtimeContext;

  /**
   * Function that generates the {@link HoodieCompactionPlan}.
   */
  private CompactionPlanOperator compactionPlanOperator;
  /**
   * Function that executes the compaction task.
   */
  private CompactFunction compactFunction;
  /**
   * Stream sink to handle compaction commits.
   */
  private CompactionCommitSink commitSink;

  @SuppressWarnings("checkstyle:MagicNumber")
  public CompactFunctionWrapper(Configuration conf) throws Exception {
    this.ioManager = new IOManagerAsync();
    MockEnvironment environment = new MockEnvironmentBuilder()
        .setTaskName("mockTask")
        .setManagedMemorySize(4 * MemoryManager.DEFAULT_PAGE_SIZE)
        .setIOManager(ioManager)
        .build();
    this.runtimeContext = new MockStreamingRuntimeContext(false, 1, 0, environment);
    this.conf = conf;
  }

  public void openFunction() throws Exception {
    compactionPlanOperator = new CompactionPlanOperator(conf);
    compactionPlanOperator.open();

    compactFunction = new CompactFunction(conf);
    compactFunction.setRuntimeContext(runtimeContext);
    compactFunction.open(conf);
    final NonThrownExecutor syncExecutor = new MockCoordinatorExecutor(
        new MockOperatorCoordinatorContext(new OperatorID(), 1));
    compactFunction.setExecutor(syncExecutor);

    commitSink = new CompactionCommitSink(conf);
    commitSink.setRuntimeContext(runtimeContext);
    commitSink.open(conf);
  }

  public void compact(long checkpointID) throws Exception {
    // collect the CompactEvents.
    CollectorOutput<CompactionPlanEvent> output = new CollectorOutput<>();
    compactionPlanOperator.setOutput(output);
    compactionPlanOperator.notifyCheckpointComplete(checkpointID);
    // collect the CompactCommitEvents
    List<CompactionCommitEvent> compactCommitEvents = new ArrayList<>();
    for (CompactionPlanEvent event : output.getRecords()) {
      compactFunction.processElement(event, null, new Collector<CompactionCommitEvent>() {
        @Override
        public void collect(CompactionCommitEvent event) {
          compactCommitEvents.add(event);
        }

        @Override
        public void close() {

        }
      });
    }
    // handle and commit the compaction
    for (CompactionCommitEvent event : compactCommitEvents) {
      commitSink.invoke(event, null);
    }
  }

  public void close() throws Exception {
    ioManager.close();
  }
}
