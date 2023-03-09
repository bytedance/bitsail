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

package com.bytedance.bitsail.entry.flink.deployment.remote;

import com.bytedance.bitsail.client.api.command.BaseCommandArgs;
import com.bytedance.bitsail.client.api.utils.PackageResolver;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.entry.flink.command.FlinkCommandArgs;
import com.bytedance.bitsail.entry.flink.deployment.DeploymentSupplier;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Run flink job in flink session.
 */
public class RemoteDeploymentSupplier implements DeploymentSupplier {
  private static final Logger LOG = LoggerFactory.getLogger(RemoteDeploymentSupplier.class);
  private static final String DEPLOYMENT_REMOTE = "remote";

  private FlinkCommandArgs flinkCommandArgs;

  @Override
  public boolean accept(FlinkCommandArgs flinkCommandArgs) {
    String deploymentMode = flinkCommandArgs.getDeploymentMode().toLowerCase().trim();
    return deploymentMode.equals(DEPLOYMENT_REMOTE);
  }

  @Override
  public void configure(FlinkCommandArgs flinkCommandArgs, BitSailConfiguration jobConfiguration) {
    this.flinkCommandArgs = flinkCommandArgs;
  }

  @Override
  public void addProperties(BaseCommandArgs baseCommandArgs, List<String> flinkCommands) {
    String jobManagerAddress = flinkCommandArgs.getJobManagerAddress();
    if (StringUtils.isNotEmpty(jobManagerAddress)) {
      flinkCommands.add("-m");
      flinkCommands.add(jobManagerAddress);
    } else {
      LOG.info("Job manager is not specified. Job will be submit to default job manager.");
    }
  }

  @Override
  public void addRunJarAndJobConfCommands(BaseCommandArgs baseCommandArgs, List<String> flinkCommands) {
    flinkCommands.add(PackageResolver.getLibraryDir().resolve(ENTRY_JAR_NAME).toString());
    if (org.apache.commons.lang.StringUtils.isNotBlank(baseCommandArgs.getJobConf())) {
      flinkCommands.add("-xjob_conf");
      flinkCommands.add(baseCommandArgs.getJobConf());
    }
    if (org.apache.commons.lang.StringUtils.isNotBlank(baseCommandArgs.getJobConfInBase64())) {
      flinkCommands.add("-xjob_conf_in_base64");
      flinkCommands.add(baseCommandArgs.getJobConfInBase64());
    }
  }

  @Override
  public void addStopProperties(BaseCommandArgs baseCommandArgs, List<String> flinkCommands) {
  }
}
