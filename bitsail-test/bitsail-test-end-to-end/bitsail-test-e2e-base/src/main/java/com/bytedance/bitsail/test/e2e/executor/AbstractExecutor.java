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

import com.bytedance.bitsail.base.version.VersionHolder;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.option.CommonOptions;
import com.bytedance.bitsail.common.util.Preconditions;
import com.bytedance.bitsail.test.e2e.base.AbstractContainer;
import com.bytedance.bitsail.test.e2e.base.transfer.TransferableFile;

import lombok.Data;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;

/**
 * Executor for running BitSail job.
 */
@Data
public abstract class AbstractExecutor extends AbstractContainer {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractExecutor.class);

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
  @Getter
  protected static String localRootDir;

  static {
    // init project root dir
    String envRootDir = System.getenv(AbstractExecutor.BITSAIL_ROOT_DIR);
    if (envRootDir != null && new File(envRootDir).exists()) {
      localRootDir = envRootDir;
    } else {
      try {
        Path curPath = Paths.get(System.getProperty("user.dir"));
        Path projectDir = curPath.getParent().getParent().getParent();
        Path outputDir = projectDir.resolve("output");
        localRootDir = outputDir.toAbsolutePath().toString();
      } catch (Exception e) {
        throw new IllegalStateException("Failed to locate project dir.", e);
      }
    }
    Preconditions.checkState(new File(localRootDir).exists());
    LOG.info("Detect project root dir: {}", localRootDir);
  }

  /**
   * Configure the executor before initialization.
   */
  public void configure(BitSailConfiguration executorConf) {
    this.revision = VersionHolder.getHolder().getBuildVersion();
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
