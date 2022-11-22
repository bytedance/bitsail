/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bytedance.bitsail.core.job;

import com.bytedance.bitsail.base.connector.reader.DataReaderDAGBuilder;
import com.bytedance.bitsail.base.connector.transformer.DataTransformDAGBuilder;
import com.bytedance.bitsail.base.connector.writer.DataWriterDAGBuilder;
import com.bytedance.bitsail.base.execution.ExecutionEnviron;
import com.bytedance.bitsail.base.execution.Mode;
import com.bytedance.bitsail.base.packages.PluginFinder;
import com.bytedance.bitsail.base.packages.PluginFinderFactory;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.configuration.ConfigParser;
import com.bytedance.bitsail.common.option.CommonOptions;
import com.bytedance.bitsail.core.command.CoreCommandArgs;
import com.bytedance.bitsail.core.execution.ExecutionEnvironFactory;
import com.bytedance.bitsail.core.reader.DataReaderBuilderFactory;
import com.bytedance.bitsail.core.writer.DataWriterBuilderFactory;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;

/**
 * Created 2022/4/21
 */
public class UnificationJob<T> implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(UnificationJob.class);

  private final BitSailConfiguration globalConfiguration;
  private final List<BitSailConfiguration> readerConfigurations;
  private final List<BitSailConfiguration> writerConfigurations;

  private final ExecutionEnviron execution;
  private final PluginFinder pluginFinder;
  private final Mode mode;

  private final long jobId;
  private final String user;
  private final String jobName;

  private List<DataReaderDAGBuilder> dataReaderDAGBuilders = Lists.newArrayList();
  private List<DataWriterDAGBuilder> dataWriterDAGBuilders = Lists.newArrayList();
  private List<DataTransformDAGBuilder> dataTransformDAGBuilders = Lists.newArrayList();

  public UnificationJob(BitSailConfiguration globalConfiguration, CoreCommandArgs coreCommandArgs) {
    if (globalConfiguration.fieldExists(CommonOptions.INSTANCE_ID)) {
      globalConfiguration.set(CommonOptions.INTERNAL_INSTANCE_ID, ConfigParser
          .getInstanceId(globalConfiguration) + "_" + System.currentTimeMillis());
    }

    mode = Mode.getJobRunMode(globalConfiguration.get(CommonOptions.JOB_TYPE));

    //execution init & start
    execution = ExecutionEnvironFactory
        .getExecutionEnviron(coreCommandArgs, mode, globalConfiguration);
    execution.start(globalConfiguration, mode);

    //plugin finder init & configure
    pluginFinder = PluginFinderFactory
        .getPluginFinder(globalConfiguration.get(CommonOptions.PLUGIN_EXPLORER_NAME));
    pluginFinder.configure(execution, execution.getCommonConfiguration());

    jobId = ConfigParser.getJobId(globalConfiguration);
    jobName = globalConfiguration.get(CommonOptions.JOB_NAME);
    // user = ConfigParser.getUserName(globalConfiguration);
    user = "default_user_name";

    Runtime.getRuntime().addShutdownHook(new Thread(
        () -> execution.terminal(dataReaderDAGBuilders, dataTransformDAGBuilders, dataWriterDAGBuilders),
        "Terminal"));

    this.globalConfiguration = globalConfiguration;
    this.readerConfigurations = execution.getReaderConfigurations();
    this.writerConfigurations = execution.getWriterConfigurations();
  }

  public void start() throws Exception {
    prepare();
    if (validate()) {
      process();
    }
  }

  /**
   * Prepare the job DAG and configure runtime configuration.
   *
   * @throws Exception
   */
  private void prepare() throws Exception {

    dataReaderDAGBuilders = DataReaderBuilderFactory
        .getDataReaderDAGBuilderList(mode,
            readerConfigurations,
            pluginFinder);

    dataWriterDAGBuilders = DataWriterBuilderFactory
        .getDataWriterDAGBuilderList(mode,
            writerConfigurations,
            pluginFinder);

    execution.configure(dataReaderDAGBuilders,
        dataTransformDAGBuilders,
        dataWriterDAGBuilders);
    LOG.info("Final global configuration: {}", execution.getGlobalConfiguration().desensitizedBeautify());
  }

  /**
   * Run the validation process before submitting the job to the cluster.
   */
  private boolean validate() throws Exception {
    for (DataReaderDAGBuilder readerDAG : dataReaderDAGBuilders) {
      if (!readerDAG.validate()) {
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

  /**
   * Run the job.
   *
   * @throws Exception
   */
  private void process() throws Exception {
    execution.run(dataReaderDAGBuilders,
        dataTransformDAGBuilders,
        dataWriterDAGBuilders);

    //todo remove shutdown hook after finished, to avoid shutdown hook invoked in normal exit.
  }

}

