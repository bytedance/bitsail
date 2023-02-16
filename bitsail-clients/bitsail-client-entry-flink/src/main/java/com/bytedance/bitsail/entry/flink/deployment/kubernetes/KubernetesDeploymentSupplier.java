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

package com.bytedance.bitsail.entry.flink.deployment.kubernetes;

import com.bytedance.bitsail.client.api.command.BaseCommandArgs;
import com.bytedance.bitsail.entry.flink.command.FlinkCommandArgs;
import com.bytedance.bitsail.entry.flink.deployment.DeploymentSupplier;

import java.util.List;

import static com.bytedance.bitsail.entry.flink.command.FlinkCommandArgs.KUBERNETES_CLUSTER_ID;

/**
 * Created 2022/12/23
 */
public class KubernetesDeploymentSupplier implements DeploymentSupplier {

  private FlinkCommandArgs flinkRunCommandArgs;

  private String deploymentMode;

  public KubernetesDeploymentSupplier(FlinkCommandArgs flinkRunCommandArgs) {
    this.flinkRunCommandArgs = flinkRunCommandArgs;
    this.deploymentMode = flinkRunCommandArgs.getDeploymentMode();
  }

  @Override
  public void addDeploymentMode(List<String> flinkCommands) {
    flinkCommands.add("-t");
    flinkCommands.add(deploymentMode);
  }

  @Override
  public void addRunDeploymentCommands(BaseCommandArgs baseCommandArgs) {
    baseCommandArgs.getProperties().put(KUBERNETES_CLUSTER_ID, flinkRunCommandArgs.getKubernetesClusterId());
  }

  @Override
  public void addStopDeploymentCommands(BaseCommandArgs baseCommandArgs) {
    baseCommandArgs.getProperties().put(KUBERNETES_CLUSTER_ID,
            flinkRunCommandArgs.getKubernetesClusterId());
  }
}
