/*
 *       Copyright 2022-2023 Bytedance Ltd. and/or its affiliates.
 *
 *       Licensed under the Apache License, Version 2.0 (the "License");
 *       you may not use this file except in compliance with the License.
 *       You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *       Unless required by applicable law or agreed to in writing, software
 *       distributed under the License is distributed on an "AS IS" BASIS,
 *       WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *       See the License for the specific language governing permissions and
 *       limitations under the License.
 */

package com.bytedance.bitsail.flink.core.execution;

import com.bytedance.bitsail.base.component.DefaultComponentBuilderLoader;
import com.bytedance.bitsail.base.execution.Mode;
import com.bytedance.bitsail.base.runtime.RuntimePluggable;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.option.CommonOptions;
import com.bytedance.bitsail.flink.core.option.FlinkCommonOptions;
import com.bytedance.bitsail.flink.core.runtime.restart.FailureRateRestartStrategyBuilder;
import com.bytedance.bitsail.flink.core.runtime.restart.FixedDelayRestartStrategyBuilder;
import com.bytedance.bitsail.flink.core.runtime.restart.FlinkRestartStrategyBuilder;

import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.ExecutionMode;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class DefaultExecutionModeAdapter implements ExecutionModeAdapter {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultExecutionModeAdapter.class);

  protected final BitSailConfiguration commonConfiguration;

  public DefaultExecutionModeAdapter(BitSailConfiguration commonConfiguration) {
    this.commonConfiguration = commonConfiguration;
  }

  @Override
  public StreamExecutionEnvironment createStreamExecutionEnvironment() {
    StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment
        .getExecutionEnvironment();
    ExecutionMode executionMode = ExecutionMode.valueOf(StringUtils
        .upperCase(commonConfiguration.get(FlinkCommonOptions.EXECUTION_MODE)));

    executionEnvironment.getConfig().setExecutionMode(executionMode);
    executionEnvironment.getConfig().enableObjectReuse();

    return executionEnvironment;
  }

  @Override
  public StreamTableEnvironment creatStreamTableExecution(StreamExecutionEnvironment streamExecutionEnvironment) {
    return StreamTableEnvironment.create(streamExecutionEnvironment);
  }

  @Override
  public List<RuntimePluggable> loadExecutionRuntimePlugins(Mode mode) {
    DefaultComponentBuilderLoader<RuntimePluggable> loader = new DefaultComponentBuilderLoader<>(RuntimePluggable.class);
    ArrayList<RuntimePluggable> plugins = Lists.newArrayList();
    for (RuntimePluggable runtimePluggable : loader.loadComponents()) {
      if (runtimePluggable.accept(mode)) {
        plugins.add(runtimePluggable);
      }
    }
    return plugins;
  }

  @Override
  public void applyExecutionModeSettings(Mode mode,
                                         FlinkExecutionEnviron flinkExecutionEnviron,
                                         StreamExecutionEnvironment streamExecutionEnvironment) {
    applyCheckpointSetting(mode, flinkExecutionEnviron, streamExecutionEnvironment);
    applyExecutionEnvironmentSettings(mode, flinkExecutionEnviron, streamExecutionEnvironment);
  }

  private void applyCheckpointSetting(Mode mode,
                                      FlinkExecutionEnviron flinkExecutionEnviron,
                                      StreamExecutionEnvironment streamExecutionEnvironment) {
    boolean checkpointEnabled = commonConfiguration.get(CommonOptions.CheckPointOptions.CHECKPOINT_ENABLE);
    if (checkpointEnabled || Mode.STREAMING.equals(mode)) {
      long checkpointInterval = commonConfiguration.get(CommonOptions.CheckPointOptions.CHECKPOINT_INTERVAL);
      long checkpointTimeout = commonConfiguration.get(CommonOptions.CheckPointOptions.CHECKPOINT_TIMEOUT);
      int tolerableFailureNumber = getDefaultCheckpointTolerableFailureNumber(flinkExecutionEnviron.getFlinkConfiguration(), commonConfiguration);
      LOG.info("Checkpoint tolerable failure number: {}.", tolerableFailureNumber);

      streamExecutionEnvironment.enableCheckpointing(checkpointInterval);
      if (tolerableFailureNumber > 0) {
        streamExecutionEnvironment.getCheckpointConfig().setTolerableCheckpointFailureNumber(tolerableFailureNumber);
      }
      streamExecutionEnvironment.getCheckpointConfig().setCheckpointTimeout(checkpointTimeout);
    }
  }

  private void applyExecutionEnvironmentSettings(Mode mode,
                                                 FlinkExecutionEnviron flinkExecutionEnviron,
                                                 StreamExecutionEnvironment streamExecutionEnvironment) {
    FlinkRestartStrategyBuilder builder = new FixedDelayRestartStrategyBuilder();
    if (Mode.STREAMING.equals(mode)) {
      builder = new FailureRateRestartStrategyBuilder();
      streamExecutionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

      int globalParallelism = flinkExecutionEnviron.parallelismAdvisor.getGlobalParallelism();
      LOG.info("Global parallelism is {}.", globalParallelism);
      streamExecutionEnvironment.setParallelism(globalParallelism);

      int userConfigMaxParallelism = commonConfiguration.getUnNecessaryOption(FlinkCommonOptions.FLINK_MAX_PARALLELISM, -1);
      if (userConfigMaxParallelism > 0) {
        streamExecutionEnvironment.setMaxParallelism(userConfigMaxParallelism);
        LOG.info("Global max parallelism is {}.", userConfigMaxParallelism);
      }
    }

    builder.setCommonConf(commonConfiguration);
    builder.setParallelismAdvisor(flinkExecutionEnviron.getParallelismAdvisor());
    builder.setExecutionMode(ExecutionMode.valueOf(StringUtils
        .upperCase(commonConfiguration.get(FlinkCommonOptions.EXECUTION_MODE))));

    RestartStrategies.RestartStrategyConfiguration restartStrategyConfiguration = builder.buildRestartStrategyConf();
    if (Objects.nonNull(restartStrategyConfiguration)) {
      streamExecutionEnvironment.setRestartStrategy(restartStrategyConfiguration);
    }
  }

  /**
   * get checkpoint config from jobConf or execution environment
   */
  private static int getDefaultCheckpointTolerableFailureNumber(Configuration configuration,
                                                                BitSailConfiguration commonConfiguration) {
    if (commonConfiguration.fieldExists(CommonOptions.CheckPointOptions.CHECKPOINT_TOLERABLE_FAILURE_NUMBER_KEY)) {
      return commonConfiguration.get(CommonOptions.CheckPointOptions.CHECKPOINT_TOLERABLE_FAILURE_NUMBER_KEY);
    }
    return -1;
  }
}
