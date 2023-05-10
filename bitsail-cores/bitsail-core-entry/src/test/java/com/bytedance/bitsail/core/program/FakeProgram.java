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

package com.bytedance.bitsail.core.program;

import com.bytedance.bitsail.base.connector.reader.DataReaderDAGBuilder;
import com.bytedance.bitsail.base.connector.transform.DataTransformDAGBuilder;
import com.bytedance.bitsail.base.connector.writer.DataWriterDAGBuilder;
import com.bytedance.bitsail.base.execution.ExecutionEnviron;
import com.bytedance.bitsail.base.execution.Mode;
import com.bytedance.bitsail.base.packages.PluginFinder;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.core.api.program.UnifiedProgram;
import com.bytedance.bitsail.core.api.program.factory.ProgramDAGBuilderFactory;

import com.google.common.collect.Lists;

import java.util.List;

public class FakeProgram extends UnifiedProgram {
  @Override
  public String getComponentName() {
    return "fake-unified-program";
  }

  @Override
  public ProgramDAGBuilderFactory createProgramBuilderFactory() {
    return new ProgramDAGBuilderFactory() {
      @Override
      public List<DataReaderDAGBuilder> getDataReaderDAGBuilders(Mode mode, List<BitSailConfiguration> readerConfigurations, PluginFinder pluginFinder) {
        return Lists.newArrayList((DataReaderDAGBuilder) () -> "fake-data-reader");
      }

      @Override
      public List<DataWriterDAGBuilder> getDataWriterDAGBuilders(Mode mode, List<BitSailConfiguration> writerConfigurations, PluginFinder pluginFinder) {
        return Lists.newArrayList((DataWriterDAGBuilder) () -> "fake-data-writer");
      }
    };
  }

  @Override
  public ExecutionEnviron createExecutionEnviron() {
    return new ExecutionEnviron() {
      @Override
      public void beforeExecution(List<DataReaderDAGBuilder> readerBuilders, List<DataTransformDAGBuilder> transformDAGBuilders,
                                  List<DataWriterDAGBuilder> writerBuilders) {

      }

      @Override
      public void run(List<DataReaderDAGBuilder> readerBuilders, List<DataTransformDAGBuilder> transformDAGBuilders, List<DataWriterDAGBuilder> writerBuilders) {

      }

      @Override
      public void terminal(List<DataReaderDAGBuilder> readerBuilders, List<DataTransformDAGBuilder> transformDAGBuilders, List<DataWriterDAGBuilder> writerBuilders) {

      }
    };
  }
}
