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

package com.bytedance.bitsail.entry.flink.command;

import com.bytedance.bitsail.client.api.command.CommandArgs;

import com.beust.jcommander.Parameter;
import lombok.Getter;
import lombok.Setter;

import static com.bytedance.bitsail.entry.flink.deployment.kubernetes.KubernetesDeploymentSupplier.KUBERNETES_CLUSTER_ID;

/**
 * Created 2022/8/5
 */
@Getter
@Setter
public class FlinkCommandArgs implements CommandArgs {
  public static final String JOB_ID = "job-id";
  private static final String JOBMANAGER_ARCHIVE_FS_DIR = "jobmanager.archive.fs.dir";
  private static final String HISTORYSERVER_WEB_ADDRESS = "historyserver.web.address";
  private static final String HISTORYSERVER_WEB_PORT = "historyserver.web.port";
  private static final String HISTORYSERVER_ARCHIVE_FS_DIR = "historyserver.archive.fs.dir";
  private static final String HISTORYSERVER_ARCHIVE_FS_REFRESH_INTERVAL = "historyserver.archive.fs.refresh-interval";


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

  @Parameter(names = "--" + JOB_ID,
          description = "The job id of running job")
  private String jobId;

  @Parameter(names = "--historyserver.enable", description = "Enable history server or not.")
  private boolean historyServerEnable = false;

  @Parameter(names = "--" + JOBMANAGER_ARCHIVE_FS_DIR,
      description = "Directory to upload completed jobs to. Add this directory to the list of monitored directories " +
          "of the HistoryServer as well")
  private String jobmanagerArchiveFsDir = "hdfs:///completed-jobs/";

  @Parameter(names = "--" + HISTORYSERVER_WEB_ADDRESS,
      description = "The address under which the web-based HistoryServer listens.")
  private String historyServerWebAddress = "0.0.0.0";

  @SuppressWarnings("checkstyle:MagicNumber")
  @Parameter(names = "--" + HISTORYSERVER_WEB_PORT,
      description = "The port under which the web-based HistoryServer listens.")
  private int historyServerWebPort = 8082;

  @Parameter(names = "--" + HISTORYSERVER_ARCHIVE_FS_DIR,
      description = "Comma separated list of directories to monitor for completed jobs.")
  private String historyServerArchiveFsDir = "hdfs:///completed-jobs/";

  @SuppressWarnings("checkstyle:MagicNumber")
  @Parameter(names = "--" + HISTORYSERVER_ARCHIVE_FS_REFRESH_INTERVAL,
      description = "Interval in milliseconds for refreshing the monitored directories.")
  private long historyServerArchiveFsRefreshInterval = 10000;
}
