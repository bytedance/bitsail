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

import com.beust.jcommander.Parameter;
import lombok.Getter;
import lombok.Setter;

/**
 * Created 2023/2/6
 */
@Getter
@Setter
public class FlinkKubernetesCommandArgs extends FlinkRunCommandArgs {
    public static final String KUBERNETES_CONTAINER_IMAGE = "kubernetes.container.image";
    public static final String KUBERNETES_JOBMANAGER_CPU = "kubernetes.jobmanager.cpu";
    public static final String KUBERNETES_TASKMANAGER_CPU = "kubernetes.taskmanager.cpu";

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
