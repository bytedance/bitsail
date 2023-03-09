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

package com.bytedance.bitsail.entry.flink.deployment.local;

import com.bytedance.bitsail.client.api.command.BaseCommandArgs;
import com.bytedance.bitsail.client.api.utils.PackageResolver;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.entry.flink.command.FlinkCommandArgs;
import com.bytedance.bitsail.entry.flink.deployment.DeploymentSupplier;

import org.apache.commons.lang.StringUtils;

import java.util.List;

/**
 * Run flink job in local mini cluster.
 */
public class LocalDeploymentSupplier implements DeploymentSupplier {

  private static final String DEPLOYMENT_LOCAL = "local";

  @Override
  public boolean accept(FlinkCommandArgs flinkCommandArgs) {
    String deploymentMode = flinkCommandArgs.getDeploymentMode().toLowerCase().trim();
    return deploymentMode.equals(DEPLOYMENT_LOCAL);
  }

  @Override
  public void configure(FlinkCommandArgs flinkCommandArgs, BitSailConfiguration jobConfiguration) {

  }

  @Override
  public void addProperties(BaseCommandArgs baseCommandArgs, List<String> flinkCommands) {

  }

  @Override
  public void addRunJarAndJobConfCommands(BaseCommandArgs baseCommandArgs, List<String> flinkCommands) {
    flinkCommands.add(PackageResolver.getLibraryDir().resolve(ENTRY_JAR_NAME).toString());
    if (StringUtils.isNotBlank(baseCommandArgs.getJobConf())) {
      flinkCommands.add("-xjob_conf");
      flinkCommands.add(baseCommandArgs.getJobConf());
    }
    if (StringUtils.isNotBlank(baseCommandArgs.getJobConfInBase64())) {
      flinkCommands.add("-xjob_conf_in_base64");
      flinkCommands.add(baseCommandArgs.getJobConfInBase64());
    }
  }

  @Override
  public void addStopProperties(BaseCommandArgs baseCommandArgs, List<String> flinkCommands) {
  }
}
