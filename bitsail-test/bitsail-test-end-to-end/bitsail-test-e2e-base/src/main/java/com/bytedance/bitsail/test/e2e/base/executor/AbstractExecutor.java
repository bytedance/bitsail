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

package com.bytedance.bitsail.test.e2e.base.executor;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.test.e2e.base.AbstractContainer;
import com.bytedance.bitsail.test.e2e.base.E2ETestOptions;
import com.bytedance.bitsail.test.e2e.base.transfer.TransferableFile;

import java.util.ArrayList;
import java.util.List;

/**
 * Executor for running BitSail job.
 */
public abstract class AbstractExecutor extends AbstractContainer {

  /**
   * Files to copy from local resource to executor, including jar files and job configurations.
   */
  protected List<TransferableFile> transferableFiles;

  /**
   * Root dir of the project when running e2e test.
   */
  protected String projectDir;

  /**
   * Configure the executor before initialization.
   */
  void configure(BitSailConfiguration executorConf) {
    this.transferableFiles = new ArrayList<>();
    this.projectDir = executorConf.get(E2ETestOptions.PROJECT_ROOT_DIR);
  }

  /**
   * Register connector libs to transfer to executor.
   */
  abstract void registerConnector(String... connectors);

  /**
   * Register core libs.
   */
  abstract void registerEngine();

  /**
   * Register common files, like job configuration.
   */
  abstract void registerCommonFile(String... files);

  /**
   * Initialize the executor.
   */
  abstract void init();

  /**
   * Run BitSail job in the executor.
   */
  abstract void run() throws Exception;
}
