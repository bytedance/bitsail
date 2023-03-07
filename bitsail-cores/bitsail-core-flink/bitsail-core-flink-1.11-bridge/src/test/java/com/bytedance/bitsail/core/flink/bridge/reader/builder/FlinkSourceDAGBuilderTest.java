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

package com.bytedance.bitsail.core.flink.bridge.reader.builder;

import com.bytedance.bitsail.base.connector.reader.v1.Source;
import com.bytedance.bitsail.base.execution.Mode;
import com.bytedance.bitsail.base.execution.ProcessResult;
import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.model.ColumnInfo;
import com.bytedance.bitsail.common.option.CommonOptions;
import com.bytedance.bitsail.common.option.ReaderOptions;
import com.bytedance.bitsail.core.flink.bridge.reader.delegate.DelegateFlinkSource;
import com.bytedance.bitsail.core.flink.bridge.reader.delegate.DelegateFlinkSourceReader;
import com.bytedance.bitsail.core.flink.bridge.reader.delegate.operator.DelegateSourceReaderContext;
import com.bytedance.bitsail.flink.core.execution.FlinkExecutionEnviron;

import com.google.common.collect.ImmutableList;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import java.util.Map;

@Slf4j
public class FlinkSourceDAGBuilderTest {

  FlinkExecutionEnviron environ;
  BitSailConfiguration commonConf;
  BitSailConfiguration readerConf;

  Source<?, ?, ?> source;
  FlinkSourceDAGBuilder<?, ?, ?> flinkSourceDAGBuilder;

  @Before
  public void init() {
    commonConf = BitSailConfiguration.newDefault();
    commonConf.set(CommonOptions.INTERNAL_INSTANCE_ID, "123");
    commonConf.set(CommonOptions.JOB_TYPE, "streaming");

    environ = new FlinkExecutionEnviron();
    environ.configure(Mode.STREAMING, null, commonConf);

    readerConf = BitSailConfiguration.newDefault();
    readerConf.set(ReaderOptions.BaseReaderOptions.COLUMNS, ImmutableList.of(
        new ColumnInfo("id", "long"),
        new ColumnInfo("value", "string")
    ));

    source = new MockSource();
    flinkSourceDAGBuilder = new FlinkSourceDAGBuilder<>(source);
  }

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testLifeCycle() throws Exception {
    flinkSourceDAGBuilder.configure(environ, readerConf);
    Assert.assertEquals(1, flinkSourceDAGBuilder.getParallelismAdvice(null, null, null).getAdviceParallelism());

    flinkSourceDAGBuilder.configure(environ, readerConf);
    flinkSourceDAGBuilder.addSource(environ, 1);

    Map<String, Object> mockAccumulator = Mockito.mock(Map.class);
    Mockito.doReturn(1L).when(mockAccumulator).getOrDefault(Mockito.anyString(), Mockito.any());
    JobExecutionResult jobExecutionResult = Mockito.mock(JobExecutionResult.class);
    Mockito.doReturn(mockAccumulator).when(jobExecutionResult).getAllAccumulatorResults();
    ProcessResult<?> processResult = ProcessResult.builder()
        .jobExecutionResult(jobExecutionResult)
        .build();
    thrown.expect(BitSailException.class);
    thrown.expectMessage("BitSail found too many dirty records.");
    flinkSourceDAGBuilder.commit(processResult);
  }

  @Test
  public void testCreateReader() {
    DelegateSourceReaderContext sourceReaderContext = Mockito.mock(DelegateSourceReaderContext.class);
    LongCounter counter = Mockito.mock(LongCounter.class);
    Mockito.doNothing().when(counter).add(Mockito.anyLong());
    RuntimeContext runtimeContext = Mockito.mock(RuntimeContext.class);
    Mockito.doReturn(counter).when(runtimeContext).getLongCounter(Mockito.anyString());
    Mockito.doReturn(runtimeContext).when(sourceReaderContext).getRuntimeContext();

    ReaderOutput readerOutput = Mockito.mock(ReaderOutput.class);
    Mockito.doNothing().when(readerOutput).collect(Mockito.any());
    Mockito.doNothing().when(readerOutput).collect(Mockito.any(), Mockito.anyLong());

    Throwable caught = null;
    try {
      flinkSourceDAGBuilder.configure(environ, readerConf);
      DelegateFlinkSource source = flinkSourceDAGBuilder.getDelegateFlinkSource();

      SourceReader sourceReader = source.createReader(sourceReaderContext);
      Assert.assertTrue(sourceReader instanceof DelegateFlinkSourceReader);

      sourceReader.start();
      sourceReader.pollNext(readerOutput);
    } catch (Throwable t) {
      log.error("Failed to create reader", t);
      caught = t;
    }
    Assert.assertNull(caught);
  }
}
