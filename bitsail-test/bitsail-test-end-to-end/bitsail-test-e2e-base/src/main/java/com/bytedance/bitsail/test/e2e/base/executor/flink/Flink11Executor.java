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

package com.bytedance.bitsail.test.e2e.base.executor.flink;

import com.google.common.collect.Lists;
import lombok.NoArgsConstructor;

import java.nio.file.Paths;
import java.util.List;

@NoArgsConstructor
public class Flink11Executor extends AbstractFlinkExecutor {

  @Override
  protected String getContainerName() {
    return "test-container-flink-1.11.6";
  }

  @Override
  protected String getFlinkDockerImage() {
    return "flink:1.11.6";
  }

  @Override
  protected String getFlinkRootDir() {
    return "/opt/flink";
  }

  @Override
  protected List<String> getExecCommand() {
    return Lists.newArrayList(
        "bin/bitsail run",
        "--engine flink",
        "--execution-mode run",
        "--deployment-mode local",
        "--conf " + Paths.get(executorRootDir, "/jobConf.json")
    );
  }
}
