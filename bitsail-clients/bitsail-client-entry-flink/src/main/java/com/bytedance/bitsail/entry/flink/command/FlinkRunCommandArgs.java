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

package com.bytedance.bitsail.entry.flink.command;

import com.bytedance.bitsail.client.api.command.CommandArgs;

import com.beust.jcommander.Parameter;
import lombok.Getter;
import lombok.Setter;

/**
 * Created 2022/8/5
 */
@Getter
@Setter
public class FlinkRunCommandArgs implements CommandArgs {

  public static final String KUBERNETES_CLUSTER_ID = "kubernetes.cluster-id";
  public static final String KUBERNETES_CONTAINER_IMAGE = "kubernetes.container.image";
  public static final String KUBERNETES_JOBMANAGER_CPU = "kubernetes.jobmanager.cpu";
  public static final String KUBERNETES_TASKMANAGER_CPU = "kubernetes.taskmanager.cpu";


  @Parameter(names = "--execution-mode",
      required = true,
      description = "Flink run action, eg: run or run-application")
  private String executionMode;

  @Parameter(names = {"--queue"},
      description = "Yarn queue's name when you use yarn deployment mode.")
  private String queue;

  @Parameter(names = "--deployment-mode",
      required = true,
      description = "Specify the flink deployment mode, eg: yarn-per-job or yarn-session")
  private String deploymentMode;

  @Parameter(names = "--from-savepoint")
  private String fromSavepoint;

  @Parameter(names = "--skip-savepoint")
  private boolean skipSavepoint;

  @SuppressWarnings("checkstyle:MagicNumber")
  @Parameter(names = "--priority",
      description = "Specify the job's priority in resource manager, eg: yarn.")
  private int priority = 5;

  @Parameter(names = "--jm-address",
      description = "Specify the job manager to use, eg: localhost:8081.")
  private String jobManagerAddress;

  @Parameter(names = "--" + KUBERNETES_CLUSTER_ID,
          description = "The cluster-id of kubernetes")
  private String kubernetesClusterId = "bitsail-job";

  @Parameter(names = "--" + KUBERNETES_CONTAINER_IMAGE,
          description = "The container image of kubernetes")
  private String kubernetesContainerImage;

  @SuppressWarnings("checkstyle:MagicNumber")
  @Parameter(names = "--" + KUBERNETES_JOBMANAGER_CPU,
          description = "The number (Double) of cpu used by job manager")
  private double kubernetesJobManagerCpu = 0.5;

  @SuppressWarnings("checkstyle:MagicNumber")
  @Parameter(names = "--" + KUBERNETES_TASKMANAGER_CPU,
          description = "The number (Double) of cpu used by task manager. By default, " +
                  "the cpu is set to the number of slots per TaskManager")
  private double kubernetesTaskManagerCpu = 0.5;
}
