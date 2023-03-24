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

import com.bytedance.bitsail.base.execution.Mode;
import com.bytedance.bitsail.base.runtime.RuntimePluggable;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.Serializable;
import java.util.List;

public interface ExecutionModeAdapter extends Serializable {

  /**
   * Create Streaming Execution Environment.
   */
  StreamExecutionEnvironment createStreamExecutionEnvironment();

  /**
   * Create Streaming Table Environment
   */
  StreamTableEnvironment creatStreamTableExecution(StreamExecutionEnvironment streamExecutionEnvironment);

  /**
   * Load Execution runtime plugins.
   */
  List<RuntimePluggable> loadExecutionRuntimePlugins(Mode mode);

  /**
   * Apply the execution setting according the mode.
   */
  void applyExecutionModeSettings(Mode mode,
                                  FlinkExecutionEnviron flinkExecutionEnviron,
                                  StreamExecutionEnvironment streamExecutionEnvironment);

}
