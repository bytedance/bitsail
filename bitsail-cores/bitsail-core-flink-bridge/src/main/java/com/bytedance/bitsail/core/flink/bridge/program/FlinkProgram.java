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

package com.bytedance.bitsail.core.flink.bridge.program;

import com.bytedance.bitsail.base.execution.ExecutionEnviron;
import com.bytedance.bitsail.core.api.program.UnifiedProgram;
import com.bytedance.bitsail.core.api.program.factory.ProgramDAGBuilderFactory;
import com.bytedance.bitsail.flink.core.execution.FlinkExecutionEnviron;

public class FlinkProgram extends UnifiedProgram {

  @Override
  public ProgramDAGBuilderFactory createProgramBuilderFactory() {
    return new FlinkDAGBuilderFactory();
  }

  @Override
  public ExecutionEnviron createExecutionEnviron() {
    return new FlinkExecutionEnviron();
  }

  @Override
  public String getComponentName() {
    return "flink";
  }
}
