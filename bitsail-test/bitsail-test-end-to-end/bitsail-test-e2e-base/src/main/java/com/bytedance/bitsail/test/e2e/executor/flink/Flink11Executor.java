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

package com.bytedance.bitsail.test.e2e.executor.flink;

import com.bytedance.bitsail.base.version.VersionHolder;
import com.bytedance.bitsail.test.e2e.base.transfer.TransferableFile;

import com.google.common.collect.Lists;
import lombok.NoArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
import java.util.List;

@NoArgsConstructor
public class Flink11Executor extends AbstractFlinkExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(Flink11Executor.class);

  @Override
  public String getContainerName() {
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

  @Override
  protected void addEngineLibs() {
    String buildVersion = VersionHolder.getHolder().getBuildVersion();

    // libs/engines/bitsail-engine-flink-{revision}.jar
    String flinkEngineFile = "bitsail-engine-flink-" + buildVersion + ".jar";
    String flinkEngine = Paths.get(localRootDir,
        "bitsail-cores",
        "bitsail-core-flink-bridge",
        "target", flinkEngineFile).toAbsolutePath().toString();
    transferableFiles.add(new TransferableFile(flinkEngine,
        Paths.get(executorRootDir, "libs", "engines", flinkEngineFile).toAbsolutePath().toString()));

    // libs/engines/mapping/bitsail-engine-flink.json
    String mappingFile = "bitsail-engine-flink.json";
    String mapping = Paths.get(localRootDir,
        "bitsail-cores",
        "bitsail-core-flink-bridge",
        "src", "main", "resources", mappingFile).toAbsolutePath().toString();
    transferableFiles.add(new TransferableFile(mapping,
        Paths.get(executorRootDir, "libs", "engines", "mapping", mappingFile).toAbsolutePath().toString()));
    LOG.info("Successfully add libs for flink engine.");

    // libs/clients/bitsail-client-entry-{engine}-{revision}.jar
    String clientEngineFile = "bitsail-client-entry-flink-" + buildVersion + ".jar";
    String clientEngine = Paths.get(localRootDir,
        "bitsail-clients",
        "bitsail-client-entry-flink",
        "target", clientEngineFile).toAbsolutePath().toString();
    transferableFiles.add(new TransferableFile(clientEngine,
        Paths.get(executorRootDir, "libs", "clients", clientEngineFile).toAbsolutePath().toString()));
    LOG.info("Successfully add libs for flink clients.");
  }
}
