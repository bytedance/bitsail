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

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.bytedance.bitsail.entry.flink.command.FlinkCommandArgs.KUBERNETES_CLUSTER_ID;
import static com.bytedance.bitsail.entry.flink.deployment.DeploymentSupplierFactory.DEPLOYMENT_KUBERNETES_APPLICATION;
import static org.junit.Assert.assertEquals;

public class KubernetesDeploymentSupplierTest {
  FlinkCommandArgs flinkRunCommandArgs;
  BaseCommandArgs baseCommandArgs;
  List<String> flinkCommands;

  @Before
  public void setup() {
    flinkRunCommandArgs = new FlinkCommandArgs();
    baseCommandArgs = new BaseCommandArgs();
    flinkCommands = new ArrayList<>();
  }

  @Test
  public void testAddRunDeploymentCommands() {
    flinkRunCommandArgs.setDeploymentMode(DEPLOYMENT_KUBERNETES_APPLICATION);
    flinkRunCommandArgs.setKubernetesClusterId("testClusterId");
    flinkRunCommandArgs.setKubernetesContainerImage("testImage");
    flinkRunCommandArgs.setKubernetesJobManagerCpu(0.5);
    flinkRunCommandArgs.setKubernetesTaskManagerCpu(0.5);
    final KubernetesDeploymentSupplier deploymentSupplier = new KubernetesDeploymentSupplier(flinkRunCommandArgs);

    deploymentSupplier.addDeploymentMode(flinkCommands);
    deploymentSupplier.addRunDeploymentCommands(baseCommandArgs);
    assertEquals(flinkCommands.size(), 2);
    assertEquals(flinkCommands.get(1), DEPLOYMENT_KUBERNETES_APPLICATION);

    assertEquals(baseCommandArgs.getProperties().size(), 5);
  }

  @Test
  public void testAddStopDeploymentCommands() {
    flinkRunCommandArgs.setDeploymentMode(DEPLOYMENT_KUBERNETES_APPLICATION);
    flinkRunCommandArgs.setKubernetesClusterId("testClusterId");
    final KubernetesDeploymentSupplier deploymentSupplier = new KubernetesDeploymentSupplier(flinkRunCommandArgs);
    deploymentSupplier.addDeploymentMode(flinkCommands);
    deploymentSupplier.addStopDeploymentCommands(baseCommandArgs);
    assertEquals(flinkCommands.size(), 2);
    assertEquals(flinkCommands.get(1), DEPLOYMENT_KUBERNETES_APPLICATION);

    assertEquals(baseCommandArgs.getProperties().size(), 1);
    assertEquals(baseCommandArgs.getProperties().get(KUBERNETES_CLUSTER_ID), "testClusterId");
  }
}
