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
import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.option.CommonOptions;
import com.bytedance.bitsail.entry.flink.command.FlinkCommandArgs;
import com.bytedance.bitsail.entry.flink.deployment.DeploymentSupplier;

import org.apache.commons.lang3.StringUtils;

import java.util.List;

import static com.bytedance.bitsail.common.exception.CommonErrorCode.CONFIG_ERROR;

/**
 * Created 2022/12/23
 */
public class KubernetesDeploymentSupplier implements DeploymentSupplier {
  public static final String DEPLOYMENT_KUBERNETES_APPLICATION = "kubernetes-application";

  public static final String KUBERNETES_CLUSTER_ID = "kubernetes.cluster-id";
  public static final String KUBERNETES_CLUSTER_JAR_PATH = "kubernetes.cluster.jar.path";

  private BitSailConfiguration jobConfiguration;
  private FlinkCommandArgs flinkRunCommandArgs;

  @Override
  public boolean accept(FlinkCommandArgs flinkCommandArgs) {
    String deploymentMode = flinkCommandArgs.getDeploymentMode().toLowerCase().trim();
    return deploymentMode.equals(DEPLOYMENT_KUBERNETES_APPLICATION);
  }

  @Override
  public void configure(FlinkCommandArgs flinkCommandArgs, BitSailConfiguration jobConfiguration) {
    this.flinkRunCommandArgs = flinkCommandArgs;
    this.jobConfiguration = jobConfiguration;
  }

  @Override
  public void addProperties(BaseCommandArgs baseCommandArgs, List<String> flinkCommands) {
    String kubernetesClusterId = baseCommandArgs.getProperties().get(KUBERNETES_CLUSTER_ID);
    if (StringUtils.isBlank(kubernetesClusterId)) {
      baseCommandArgs.getProperties().put(KUBERNETES_CLUSTER_ID,
          "bitsail-" + jobConfiguration.get(CommonOptions.INSTANCE_ID));
    }
  }

  @Override
  public void addRunJarAndJobConfCommands(BaseCommandArgs baseCommandArgs, List<String> flinkCommands) {
    /*
     * Customize path of BitSail JAR file for Kubernetes application mode.
     * Only 'local://' is supported as schema for Flink application mode. This assumes
     * that the jar is located in the image, not the Flink client.
     * More details in
     * https://nightlies.apache.org/flink/flink-docs-release-1.11/ops/deployment/native_kubernetes.html#start-flink-application
     */
    flinkCommands.add("local://" + flinkRunCommandArgs.getKubernetesClusterJarPath());
    if (StringUtils.isNotBlank(baseCommandArgs.getJobConfInBase64())) {
      flinkCommands.add("-xjob_conf_in_base64");
      flinkCommands.add(baseCommandArgs.getJobConfInBase64());
    }
  }

  @Override
  public void addStopProperties(BaseCommandArgs baseCommandArgs, List<String> flinkCommands) {
    String kubernetesClusterId = baseCommandArgs.getProperties().get(KUBERNETES_CLUSTER_ID);
    if (StringUtils.isBlank(kubernetesClusterId)) {
      throw new BitSailException(CONFIG_ERROR, "Missing kubernetes cluster-id. Not able to stop the application");
    }
    if (StringUtils.isBlank(flinkRunCommandArgs.getJobId())) {
      throw new BitSailException(CONFIG_ERROR, "Missing kubernetes jobId. Not able to stop the application");
    }
    flinkCommands.add("-D");
    flinkCommands.add(KUBERNETES_CLUSTER_ID + "=" + kubernetesClusterId);
    flinkCommands.add(flinkRunCommandArgs.getJobId());
  }
}
