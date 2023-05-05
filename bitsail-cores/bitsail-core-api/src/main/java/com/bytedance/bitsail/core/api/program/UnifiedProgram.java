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
import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.configuration.ConfigParser;
import com.bytedance.bitsail.common.exception.CommonErrorCode;
import com.bytedance.bitsail.common.option.CommonOptions;
import com.bytedance.bitsail.core.api.command.CoreCommandArgs;
import com.bytedance.bitsail.core.api.program.factory.ProgramDAGBuilderFactory;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public abstract class UnifiedProgram implements Program {
  private static final Logger LOG = LoggerFactory.getLogger(UnifiedProgram.class);

  private BitSailConfiguration globalConfiguration;
  private List<BitSailConfiguration> readerConfigurations;
  private List<BitSailConfiguration> transformConfigurations;
  private List<BitSailConfiguration> writerConfigurations;

  private ExecutionEnviron execution;
  private PluginFinder pluginFinder;
  private Mode mode;

  private long jobId;
  private String user;
  private String jobName;

  private List<DataReaderDAGBuilder> dataReaderDAGBuilders = Lists.newArrayList();
  private List<DataTransformDAGBuilder> dataTransformDAGBuilders = Lists.newArrayList();
  private List<DataWriterDAGBuilder> dataWriterDAGBuilders = Lists.newArrayList();
  @Override
  public void configure(PluginFinder pluginFinder,
                        BitSailConfiguration globalConfiguration,
                        CoreCommandArgs coreCommandArgs) throws Exception {
    if (globalConfiguration.fieldExists(CommonOptions.INSTANCE_ID)) {
      globalConfiguration.set(CommonOptions.INTERNAL_INSTANCE_ID, ConfigParser
          .getInstanceId(globalConfiguration) + "_" + System.currentTimeMillis());
    }
    //plugin finder init & configure
    this.pluginFinder = pluginFinder;
    mode = Mode.getJobRunMode(globalConfiguration.get(CommonOptions.JOB_TYPE));

    //execution init & start
    execution = createExecutionEnviron();
    execution.configure(mode, pluginFinder, globalConfiguration);

    jobId = ConfigParser.getJobId(globalConfiguration);
    jobName = globalConfiguration.get(CommonOptions.JOB_NAME);
    // user = ConfigParser.getUserName(globalConfiguration);
    user = "default_user_name";

    Runtime.getRuntime().addShutdownHook(new Thread(
        () -> execution.terminal(dataReaderDAGBuilders, dataTransformDAGBuilders, dataWriterDAGBuilders),
        "Terminal"));

    this.globalConfiguration = globalConfiguration;
    this.readerConfigurations = execution.getReaderConfigurations();
    this.transformConfigurations = execution.getTransformConfigurations();
    this.writerConfigurations = execution.getWriterConfigurations();

    prepare();
  }

  private void prepare() throws Exception {
    ProgramDAGBuilderFactory programBuilderFactory = createProgramBuilderFactory();

    dataReaderDAGBuilders = programBuilderFactory
        .getDataReaderDAGBuilders(mode,
            readerConfigurations,
            pluginFinder);

    dataTransformDAGBuilders = programBuilderFactory
        .getDataTransformDAGBuilders(mode,
            transformConfigurations,
            pluginFinder);

    dataWriterDAGBuilders = programBuilderFactory
        .getDataWriterDAGBuilders(mode,
            writerConfigurations,
            pluginFinder);

    execution.beforeExecution(dataReaderDAGBuilders,
        dataTransformDAGBuilders,
        dataWriterDAGBuilders);
    LOG.info("Final global configuration: {}", execution.getGlobalConfiguration().desensitizedBeautify());
  }

  @Override
  public void submit() throws Exception {
    try {
      validate();
    } catch (Exception e) {
      throw BitSailException.asBitSailException(CommonErrorCode.DAG_VALIDATION_EXCEPTION, e);
    }
    execution.run(dataReaderDAGBuilders,
        dataTransformDAGBuilders,
        dataWriterDAGBuilders);
    //todo remove shutdown hook after finished, to avoid shutdown hook invoked in normal exit.
  }

  @Override
  public boolean validate() throws Exception {
    for (DataReaderDAGBuilder readerDAG : dataReaderDAGBuilders) {
      if (!readerDAG.validate()) {
        return false;
      }
    }
    for (DataTransformDAGBuilder transformDAG : dataTransformDAGBuilders) {
      if (!transformDAG.validate()) {
        return false;
      }
    }
    for (DataWriterDAGBuilder writerDAG : dataWriterDAGBuilders) {
      if (!writerDAG.validate()) {
        return false;
      }
    }
    return true;
  }
}
