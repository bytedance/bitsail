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

package com.bytedance.bitsail.flink.core.runtime.restart;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.flink.core.parallelism.FlinkParallelismAdvisor;

import lombok.Setter;
import org.apache.flink.api.common.ExecutionMode;
import org.apache.flink.api.common.restartstrategy.RestartStrategies.RestartStrategyConfiguration;

@Setter
public abstract class FlinkRestartStrategyBuilder {

  protected BitSailConfiguration commonConf;

  protected FlinkParallelismAdvisor parallelismAdvisor;

  protected ExecutionMode executionMode;

  public abstract  RestartStrategyConfiguration buildRestartStrategyConf();
}
