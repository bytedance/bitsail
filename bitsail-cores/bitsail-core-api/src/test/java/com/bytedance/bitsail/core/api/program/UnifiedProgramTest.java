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

package com.bytedance.bitsail.core.api.program;

import com.bytedance.bitsail.base.connector.reader.DataReaderDAGBuilder;
import com.bytedance.bitsail.base.connector.transform.DataTransformDAGBuilder;
import com.bytedance.bitsail.base.connector.writer.DataWriterDAGBuilder;
import com.bytedance.bitsail.base.execution.ExecutionEnviron;
import com.bytedance.bitsail.base.execution.Mode;
import com.bytedance.bitsail.base.packages.PluginFinder;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.option.CommonOptions;
import com.bytedance.bitsail.core.api.program.factory.ProgramDAGBuilderFactory;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class UnifiedProgramTest {

  @Test
  public void testExecFakeProgram() throws Exception {
    BitSailConfiguration jobConf = BitSailConfiguration.newDefault();
    jobConf.set(CommonOptions.JOB_ID, 123L);
    UnifiedProgram program = new FakeProgram();
    program.configure(null, jobConf, null);
    program.submit();
    Assert.assertTrue(program.validate());
  }

  static class FakeProgram extends UnifiedProgram {
    @Override
    public String getComponentName() {
      return "fake-unified-program";
    }

    @Override
    public ProgramDAGBuilderFactory createProgramBuilderFactory() {
      return new ProgramDAGBuilderFactory() {
        @Override
        public List<DataReaderDAGBuilder> getDataReaderDAGBuilders(Mode mode,
                                                                   List<BitSailConfiguration> readerConfigurations,
                                                                   PluginFinder pluginFinder) {
          return Lists.newArrayList((DataReaderDAGBuilder) () -> "fake-data-reader");
        }

        @Override
        public List<DataWriterDAGBuilder> getDataWriterDAGBuilders(Mode mode,
                                                                   List<BitSailConfiguration> writerConfigurations,
                                                                   PluginFinder pluginFinder) {
          return Lists.newArrayList((DataWriterDAGBuilder) () -> "fake-data-writer");
        }
      };
    }

    @Override
    public ExecutionEnviron createExecutionEnviron() {
      return new FakeExecutionEnviron();
    }
  }

  static class FakeExecutionEnviron extends ExecutionEnviron {
    private static final Logger LOG = LoggerFactory.getLogger(FakeExecutionEnviron.class);

    @Override
    public void beforeExecution(List<DataReaderDAGBuilder> readerBuilders,
                                List<DataTransformDAGBuilder> transformDAGBuilders,
                                List<DataWriterDAGBuilder> writerBuilders) {
      LOG.info("Reach 'before execution' phase");
    }

    @Override
    public void run(List<DataReaderDAGBuilder> readerBuilders, List<DataTransformDAGBuilder> transformDAGBuilders, List<DataWriterDAGBuilder> writerBuilders) {
      LOG.info("Running.");
    }

    @Override
    public void terminal(List<DataReaderDAGBuilder> readerBuilders, List<DataTransformDAGBuilder> transformDAGBuilders, List<DataWriterDAGBuilder> writerBuilders) {
      LOG.info("Terminated.");
    }
  }
}
