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

package com.bytedance.bitsail.test.e2e.executor;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.option.CommonOptions;
import com.bytedance.bitsail.test.e2e.base.AbstractContainer;
import com.bytedance.bitsail.test.e2e.base.transfer.TransferableFile;

import lombok.Data;

import java.util.HashSet;
import java.util.Set;

/**
 * Executor for running BitSail job.
 */
@Data
public abstract class AbstractExecutor extends AbstractContainer {

  public static final String BITSAIL_REVISION = "bitsail.revision";
  public static final String BITSAIL_ROOT_DIR = "bitsail.rootDir";

  /**
   * Revision of bitsail libs.
   */
  protected String revision;

  /**
   * Files to copy from local resource to executor, including jar files and job configurations.
   */
  protected Set<TransferableFile> transferableFiles;

  /**
   * Root dir of the project in executor when running e2e test.
   */
  protected String executorRootDir;

  /**
   * Local project dir.
   */
  protected String localRootDir;

  /**
   * Configure the executor before initialization.
   */
  public void configure(BitSailConfiguration executorConf) {
    this.revision = System.getenv(BITSAIL_REVISION);
    this.localRootDir = System.getenv(BITSAIL_ROOT_DIR);
    this.transferableFiles = new HashSet<>();
    this.executorRootDir = executorConf.get(CommonOptions.E2EOptions.E2E_EXECUTOR_ROOT_DIR);
  }

  /**
   * Initialize the executor.
   */
  public abstract void init();

  /**
   * Run BitSail job in the executor.
   * @param testId Identification of current test case.
   * @return Exit code of bitsail program.
   */
  public abstract int run(String testId) throws Exception;
}
