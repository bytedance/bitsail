/*
 * Copyright 2022 Bytedance Ltd. and/or its affiliates.
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
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.entry.flink.command.FlinkKubernetesCommandArgs;
import com.bytedance.bitsail.entry.flink.command.FlinkRunCommandArgs;
import com.bytedance.bitsail.entry.flink.deployment.DeploymentSupplier;

import java.util.List;

import static com.bytedance.bitsail.entry.flink.command.FlinkKubernetesCommandArgs.*;
import static com.bytedance.bitsail.entry.flink.deployment.DeploymentSupplierFactory.DEPLOYMENT_KUBERNETES_APPLICATION;
import static com.bytedance.bitsail.entry.flink.deployment.DeploymentSupplierFactory.DEPLOYMENT_KUBERNETES_SESSION;

/**
 * Created 2022/12/23
 */
public class KubernetesDeploymentSupplier implements DeploymentSupplier {

  private FlinkKubernetesCommandArgs flinkKubernetesCommandArgs;

  private String deploymentMode;

  public KubernetesDeploymentSupplier(FlinkRunCommandArgs flinkKubernetesCommandArgs, BitSailConfiguration jobConfiguration) {
    this.flinkKubernetesCommandArgs = (FlinkKubernetesCommandArgs) flinkKubernetesCommandArgs;
    this.deploymentMode = flinkKubernetesCommandArgs.getDeploymentMode();
  }

  @Override
  public void addDeploymentCommands(BaseCommandArgs baseCommandArgs, List<String> flinkCommands) {
    flinkCommands.add("-t");
    flinkCommands.add(deploymentMode);

    baseCommandArgs.getProperties().put(KUBERNETES_CONTAINER_IMAGE, "bitsail-core:" + getDeploymentImageTag());

    baseCommandArgs.getProperties().put(KUBERNETES_JOBMANAGER_CPU,
            String.valueOf(flinkKubernetesCommandArgs.getKubernetesJobManagerCpu()));

    baseCommandArgs.getProperties().put(KUBERNETES_TASKMANAGER_CPU,
            String.valueOf(flinkKubernetesCommandArgs.getKubernetesTaskManagerCpu()));
  }

  private String getDeploymentImageTag() {
    final String imageTag;
    switch (deploymentMode) {
      case DEPLOYMENT_KUBERNETES_SESSION:
        imageTag = "sessionmode";
        break;
      case DEPLOYMENT_KUBERNETES_APPLICATION:
      default:
        imageTag = "appmode";
        break;
    }
    return imageTag;
  }
}
