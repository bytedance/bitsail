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

package com.bytedance.bitsail.flink.core.dirty;

import com.bytedance.bitsail.base.execution.ProcessResult;
import com.bytedance.bitsail.base.messenger.common.MessengerGroup;
import com.bytedance.bitsail.base.messenger.context.MessengerContext;
import com.bytedance.bitsail.base.messenger.context.SimpleMessengerContext;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;

import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.ListAccumulator;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class FlinkBatchDirtyConnectorTest {

  private final String instanceId = "1234";
  private final MessengerContext context = new SimpleMessengerContext(instanceId, MessengerGroup.READER);
  private final BitSailConfiguration commonConf = BitSailConfiguration.newDefault();

  private final String[] dirtyRecords = {
      "dirty-record-0",
      "dirty-record-1",
      "dirty-record-2",
      "dirty-record-3",
      "dirty-record-4"
  };

  private RuntimeContext runtimeContext;
  private Map<String, Accumulator> realAccumulators;

  @Before
  public void initRuntimeContext() {
    realAccumulators = new HashMap<>();

    runtimeContext = Mockito.mock(RuntimeContext.class);
    Mockito.when(runtimeContext.getAccumulator(Mockito.anyString())).thenReturn(null);

    Mockito.doAnswer((Answer<Void>) invocation -> {
      Object[] args = invocation.getArguments();
      String accumulatorName = (String) args[0];
      Accumulator accumulator = (Accumulator) args[1];
      realAccumulators.put(accumulatorName, accumulator);
      return null;
    }).when(runtimeContext).addAccumulator(Mockito.anyString(), Mockito.any(Accumulator.class));
  }

  @Test
  public void testCollectDirty() throws IOException {
    FlinkBatchDirtyCollector collector = new FlinkBatchDirtyCollector(commonConf, context);
    for (String dirtyRecord : dirtyRecords) {
      collector.collect(dirtyRecord, new Throwable(dirtyRecord), System.currentTimeMillis());
    }

    collector.setRuntimeContext(runtimeContext);
    collector.storeDirtyRecords();

    Assert.assertEquals(1, realAccumulators.size());
    Assert.assertTrue(realAccumulators.containsKey("input_1234_LIST_DIRTY"));

    ListAccumulator<?> accumulator = (ListAccumulator<String>) realAccumulators.get("input_1234_LIST_DIRTY");
    Set<?> collectedDirtyRecords = accumulator.getLocalValue().stream().collect(Collectors.toSet());
    for (String dirtyRecord : dirtyRecords) {
      String record = String.format("Row: [%s]. message: [%s]", dirtyRecord, dirtyRecord);
      Assert.assertTrue(collectedDirtyRecords.contains(record));
    }
  }

  @Test
  public void testRestoreDirtyRecords() {
    FlinkBatchDirtyCollector collector = (FlinkBatchDirtyCollector) new FlinkBatchDirtyCollectorBuilder()
        .createDirtyCollector(commonConf, context);

    JobExecutionResult result = Mockito.mock(JobExecutionResult.class);
    Mockito.when(result.getAccumulatorResult(Mockito.anyString()))
        .thenReturn(Arrays.stream(dirtyRecords)
            .map(dirtyRecord -> String.format("Row: [%s]. message: [%s]", dirtyRecord, dirtyRecord))
            .collect(Collectors.toList()));
    ProcessResult<Object> processResult = ProcessResult.builder().build();
    processResult.setJobExecutionResult(result);

    collector.restoreDirtyRecords(processResult);
    Assert.assertTrue(CollectionUtils.isEmpty(processResult.getOutputDirtyRecords()));
    Assert.assertEquals(5, processResult.getInputDirtyRecords().size());
  }

  @After
  public void destroy() {
    runtimeContext = null;
    if (realAccumulators != null) {
      realAccumulators.clear();
      realAccumulators = null;
    }
  }
}
