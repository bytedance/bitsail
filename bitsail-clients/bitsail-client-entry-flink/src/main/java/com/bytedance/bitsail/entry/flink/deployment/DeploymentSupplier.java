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

package com.bytedance.bitsail.entry.flink.deployment;

import com.bytedance.bitsail.client.api.command.BaseCommandArgs;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.entry.flink.command.FlinkCommandArgs;

import java.util.List;

/**
 * Created 2022/8/8
 */
public interface DeploymentSupplier {
  String ENTRY_JAR_NAME = "bitsail-core.jar";

  /**
   * Check whether deployment supplier could accept the input global configuration
   */
  boolean accept(FlinkCommandArgs flinkCommandArgs);

  /**
   * Configure the deployment.
   */
  void configure(FlinkCommandArgs flinkCommandArgs, BitSailConfiguration jobConfiguration);

  void addProperties(BaseCommandArgs baseCommandArgs, List<String> flinkCommands);

  void addRunJarAndJobConfCommands(BaseCommandArgs baseCommandArgs, List<String> flinkCommands);

  void addStopProperties(BaseCommandArgs baseCommandArgs, List<String> flinkCommands);
}
