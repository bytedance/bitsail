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

import org.apache.commons.lang3.StringUtils;

import java.util.List;

/**
 * Created 2022/12/23
 */
public class KubernetesDeploymentSupplier implements DeploymentSupplier {
  public static final String KUBERNETES_CLUSTER_ID = "kubernetes.cluster-id";

  private FlinkCommandArgs flinkRunCommandArgs;

  public KubernetesDeploymentSupplier(FlinkCommandArgs flinkRunCommandArgs) {
    this.flinkRunCommandArgs = flinkRunCommandArgs;
  }

  @Override
  public void addRunProperties(BaseCommandArgs baseCommandArgs, List<String> flinkCommands) {
    baseCommandArgs.getProperties().put(KUBERNETES_CLUSTER_ID, flinkRunCommandArgs.getKubernetesClusterId());
  }

  @Override
  public void addRunJarAndJobConfCommands(BaseCommandArgs baseCommandArgs, List<String> flinkCommands) {
    /*
     * Customize path of BitSail JAR file for Kubernetes application mode, because Kubernetes
     * application mode bundles BitSail JAR file together with the custom image and runs the user
     * code's main() method on the cluster. Programmed path for
     * BitSail JAR: local:///opt/flink/usrlibs/bitsail-core.jar
     * Wee more details in
     * https://nightlies.apache.org/flink/flink-docs-release-1.11/ops/deployment/native_kubernetes.html#start-flink-application
     */
    flinkCommands.add("local:///opt/flink/usrlibs/" + ENTRY_JAR_NAME);
    if (StringUtils.isNotBlank(baseCommandArgs.getJobConfInBase64())) {
      flinkCommands.add("-xjob_conf_in_base64");
      flinkCommands.add(baseCommandArgs.getJobConfInBase64());
    }
  }

  @Override
  public void addStopProperties(BaseCommandArgs baseCommandArgs, List<String> flinkCommands) {
    baseCommandArgs.getProperties().put(KUBERNETES_CLUSTER_ID, flinkRunCommandArgs.getKubernetesClusterId());
  }
}
