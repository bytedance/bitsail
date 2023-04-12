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

package com.bytedance.bitsail.entry.flink.deployment.yarn;

import com.bytedance.bitsail.client.api.command.BaseCommandArgs;
import com.bytedance.bitsail.client.api.utils.PackageResolver;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.exception.CommonErrorCode;
import com.bytedance.bitsail.common.option.CommonOptions;
import com.bytedance.bitsail.entry.flink.command.FlinkCommandArgs;
import com.bytedance.bitsail.entry.flink.deployment.DeploymentSupplier;

import org.apache.commons.lang3.StringUtils;

import java.util.List;

/**
 * Created 2022/8/8
 */
public class YarnDeploymentSupplier implements DeploymentSupplier {

  public static final String DEPLOYMENT_YARN_PER_JOB = "yarn-per-job";
  public static final String DEPLOYMENT_YARN_SESSION = "yarn-session";
  public static final String DEPLOYMENT_YARN_APPLICATION = "yarn-application";

  private FlinkCommandArgs flinkCommandArgs;

  private BitSailConfiguration jobConfiguration;

  private String deploymentMode;

  @Override
  public boolean accept(FlinkCommandArgs flinkCommandArgs) {
    String deploymentMode = flinkCommandArgs.getDeploymentMode().toLowerCase().trim();
    return deploymentMode.equals(DEPLOYMENT_YARN_PER_JOB)
        || deploymentMode.equals(DEPLOYMENT_YARN_SESSION)
        || deploymentMode.equals(DEPLOYMENT_YARN_APPLICATION);
  }

  @Override
  public void configure(FlinkCommandArgs flinkCommandArgs, BitSailConfiguration jobConfiguration) {
    this.flinkCommandArgs = flinkCommandArgs;
    this.jobConfiguration = jobConfiguration;
  }

  @Override
  public void addProperties(BaseCommandArgs baseCommandArgs, List<String> flinkCommands) {
    baseCommandArgs.getProperties()
        .put("yarn.application.name", jobConfiguration.getNecessaryOption(
            CommonOptions.JOB_NAME, CommonErrorCode.CONFIG_ERROR));

    baseCommandArgs.getProperties()
        .put("yarn.application.queue", flinkCommandArgs.getQueue());

    baseCommandArgs.getProperties()
        .put("yarn.application.priority", String.valueOf(flinkCommandArgs.getPriority()));
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
