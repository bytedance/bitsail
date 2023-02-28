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

import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;

import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static com.bytedance.bitsail.entry.flink.deployment.kubernetes.KubernetesDeploymentSupplier.DEPLOYMENT_KUBERNETES_APPLICATION;
import static com.bytedance.bitsail.entry.flink.deployment.kubernetes.KubernetesDeploymentSupplier.KUBERNETES_CLUSTER_ID;
import static org.junit.Assert.assertEquals;

public class KubernetesDeploymentSupplierTest {
  @Rule
  public EnvironmentVariables variables = new EnvironmentVariables();
  BitSailConfiguration jobConfiguration = BitSailConfiguration.newDefault();
  FlinkCommandArgs flinkRunCommandArgs;
  BaseCommandArgs baseCommandArgs;
  List<String> flinkCommands;

  @Before
  public void setup() throws URISyntaxException {
    variables.set("BITSAIL_CONF_DIR", Paths.get(KubernetesDeploymentSupplier.class.getClassLoader().getResource("").toURI()).toString());
    flinkRunCommandArgs = new FlinkCommandArgs();
    baseCommandArgs = new BaseCommandArgs();
    flinkCommands = new ArrayList<>();
  }

  @Test
  public void testAddRunDeploymentCommands() {
    flinkRunCommandArgs.setDeploymentMode(DEPLOYMENT_KUBERNETES_APPLICATION);
    baseCommandArgs.setJobConfInBase64("test");
    jobConfiguration.set(CommonOptions.INSTANCE_ID, 123L);
    final KubernetesDeploymentSupplier deploymentSupplier = new KubernetesDeploymentSupplier();
    deploymentSupplier.configure(flinkRunCommandArgs, jobConfiguration);
    deploymentSupplier.addProperties(baseCommandArgs, flinkCommands);
    deploymentSupplier.addRunJarAndJobConfCommands(baseCommandArgs, flinkCommands);
    assertEquals(ImmutableList.of(
        "local:///opt/bitsail/bitsail-core.jar",
        "-xjob_conf_in_base64",
        "test"
    ), flinkCommands);
    assertEquals("bitsail-123", baseCommandArgs.getProperties().get(KUBERNETES_CLUSTER_ID));
  }

  @Test
  public void testAddRunDeploymentCommandsWithAssignedClusterId() {
    flinkRunCommandArgs.setDeploymentMode(DEPLOYMENT_KUBERNETES_APPLICATION);
    baseCommandArgs.setJobConfInBase64("test");
    baseCommandArgs.getProperties().put(KUBERNETES_CLUSTER_ID, "clusterId");
    final KubernetesDeploymentSupplier deploymentSupplier = new KubernetesDeploymentSupplier();
    deploymentSupplier.configure(flinkRunCommandArgs, jobConfiguration);
    deploymentSupplier.addProperties(baseCommandArgs, flinkCommands);
    deploymentSupplier.addRunJarAndJobConfCommands(baseCommandArgs, flinkCommands);
    assertEquals(ImmutableList.of(
        "local:///opt/bitsail/bitsail-core.jar",
        "-xjob_conf_in_base64",
        "test"
    ), flinkCommands);
  }

  @Test(expected = BitSailException.class)
  public void testAddStopDeploymentCommandsWithoutClusterId() {
    flinkRunCommandArgs.setDeploymentMode(DEPLOYMENT_KUBERNETES_APPLICATION);
    flinkRunCommandArgs.setJobId("jobId");
    final KubernetesDeploymentSupplier deploymentSupplier = new KubernetesDeploymentSupplier();
    deploymentSupplier.configure(flinkRunCommandArgs, jobConfiguration);
    deploymentSupplier.addStopProperties(baseCommandArgs, flinkCommands);
  }

  @Test(expected = BitSailException.class)
  public void testAddStopDeploymentCommandsWithoutJobId() {
    flinkRunCommandArgs.setDeploymentMode(DEPLOYMENT_KUBERNETES_APPLICATION);
    flinkRunCommandArgs.setKubernetesClusterId("clusterId");
    final KubernetesDeploymentSupplier deploymentSupplier = new KubernetesDeploymentSupplier();
    deploymentSupplier.configure(flinkRunCommandArgs, jobConfiguration);
    deploymentSupplier.addStopProperties(baseCommandArgs, flinkCommands);
  }

  @Test
  public void testAddStopDeploymentCommands() {
    flinkRunCommandArgs.setDeploymentMode(DEPLOYMENT_KUBERNETES_APPLICATION);
    flinkRunCommandArgs.setJobId("jobId");
    baseCommandArgs.getProperties().put(KUBERNETES_CLUSTER_ID, "clusterId");
    final KubernetesDeploymentSupplier deploymentSupplier = new KubernetesDeploymentSupplier();
    deploymentSupplier.configure(flinkRunCommandArgs, jobConfiguration);
    deploymentSupplier.addStopProperties(baseCommandArgs, flinkCommands);
    assertEquals(ImmutableList.of("-D", KUBERNETES_CLUSTER_ID + "=clusterId", "jobId"), flinkCommands);
    assertEquals(baseCommandArgs.getProperties().size(), 1);
  }
}
