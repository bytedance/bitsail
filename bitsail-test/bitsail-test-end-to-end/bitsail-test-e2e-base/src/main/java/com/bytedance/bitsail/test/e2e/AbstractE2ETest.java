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

package com.bytedance.bitsail.test.e2e;

import com.bytedance.bitsail.base.version.VersionHolder;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.util.Preconditions;
import com.bytedance.bitsail.test.e2e.datasource.AbstractDataSource;
import com.bytedance.bitsail.test.e2e.executor.AbstractExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.function.Consumer;

public abstract class AbstractE2ETest {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractE2ETest.class);

  public static final String EMPTY_SOURCE = "empty";

  static {
    // init build version
    final String finalRevision;

    VersionHolder versionHolder = VersionHolder.getHolder();
    String buildVersion = versionHolder.getBuildVersion();
    if (VersionHolder.isBuildVersionValid(buildVersion)) {
      finalRevision = buildVersion;
    } else {
      finalRevision = System.getenv(AbstractExecutor.BITSAIL_REVISION);
      try {
        Field versionField = VersionHolder.class.getDeclaredField("gitBuildVersion");
        versionField.setAccessible(true);
        versionField.set(versionHolder, finalRevision);
        LOG.info("Modify build version from [{}] to [{}].", buildVersion, finalRevision);
      } catch (Exception e) {
        throw new IllegalStateException("Failed to modify git version.", e);
      }
    }
    Preconditions.checkState(VersionHolder.isBuildVersionValid(finalRevision));
    LOG.info("Detect build version: {}", finalRevision);
  }

  /**
   * @param jobConf Job conf to run.
   * @param engineType Executor engine type.
   */
  protected static void submitJob(BitSailConfiguration jobConf,
                                  String engineType,
                                  String jobName,
                                  Consumer<AbstractDataSource> validation) throws Exception {
    int exitCode;
    try (TestJob testJob = TestJob.builder()
        .withJobConf(jobConf)
        .withEngineType(engineType)
        .build()) {
      exitCode = testJob.run(jobName);
      if (exitCode != 0) {
        throw new IllegalStateException("Failed to execute job with exit code " + exitCode);
      }

      testJob.validate(validation);
    } catch (Throwable t) {
      t.printStackTrace();
      throw t;
    }
  }

  protected static void submitFlink11Job(BitSailConfiguration jobConf,
                                         String jobName) throws Exception {
    submitJob(jobConf, "flink11", jobName, null);
  }

  protected static void submitFlink11Job(BitSailConfiguration jobConf,
                                         String jobName,
                                         Consumer<AbstractDataSource> validation) throws Exception {
    submitJob(jobConf, "flink11", jobName, validation);
  }
}
