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

package com.bytedance.bitsail.test.e2e;

import com.bytedance.bitsail.base.version.VersionHolder;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.util.Preconditions;
import com.bytedance.bitsail.test.e2e.annotation.ExecutorPatterns;
import com.bytedance.bitsail.test.e2e.annotation.ReuseContainers;
import com.bytedance.bitsail.test.e2e.datasource.AbstractDataSource;
import com.bytedance.bitsail.test.e2e.executor.AbstractExecutor;

import lombok.ToString;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.compress.utils.Lists;
import org.junit.AfterClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

@ToString
@ExecutorPatterns(exclude = {"example-*"})
public abstract class AbstractE2ETest {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractE2ETest.class);

  private boolean reuseSource = false;
  private boolean reuseSink = false;
  private boolean reuseExecutor = false;

  protected List<String> includedExecutorPattern;
  protected List<String> excludedExecutorPattern;

  private static final List<TestJob> FINISHED_JOBS = new ArrayList<>();

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

  AbstractE2ETest() {
    ReuseContainers reuseContainers = getClass().getAnnotation(ReuseContainers.class);
    if (reuseContainers != null) {
      String[] reuses = reuseContainers.reuse();
      for (String reuse : reuses) {
        if ("executor".equals(reuse)) {
          reuseExecutor = true;
        }
        if ("source".equals(reuse)) {
          reuseSource = true;
        }
        if ("sink".equals(reuse)) {
          reuseSink = true;
        }
      }
    }

    ExecutorPatterns executorPatterns;
    includedExecutorPattern = Lists.newArrayList();
    excludedExecutorPattern = Lists.newArrayList();

    executorPatterns = getClass().getAnnotation(ExecutorPatterns.class);
    if (executorPatterns != null) {
      includedExecutorPattern.addAll(Arrays.asList(executorPatterns.include()));
      excludedExecutorPattern.addAll(Arrays.asList(executorPatterns.exclude()));
    }

    executorPatterns = AbstractE2ETest.class.getAnnotation(ExecutorPatterns.class);
    if (executorPatterns != null) {
      includedExecutorPattern.addAll(Arrays.asList(executorPatterns.include()));
      excludedExecutorPattern.addAll(Arrays.asList(executorPatterns.exclude()));
    }

    LOG.info("AbstractE2ETest setting: {}", this);
  }

  protected TestJob createJob(BitSailConfiguration jobConf) {
    TestJob.TestJobBuilder builder = TestJob.builder().jobConf(jobConf);
    builder = builder.includedExecutorPattern(includedExecutorPattern);
    builder = builder.excludedExecutorPattern(excludedExecutorPattern);

    if ((reuseSource || reuseSink || reuseExecutor) && CollectionUtils.isNotEmpty(FINISHED_JOBS)) {
      builder = builder.network(FINISHED_JOBS.get(0).getNetwork());
    }

    if (reuseSource && CollectionUtils.isNotEmpty(FINISHED_JOBS)) {
      builder = builder.source(FINISHED_JOBS.get(0).getSource());
    }
    if (reuseSink && CollectionUtils.isNotEmpty(FINISHED_JOBS)) {
      builder = builder.sink(FINISHED_JOBS.get(0).getSink());
    }

    if (reuseExecutor && CollectionUtils.isNotEmpty(FINISHED_JOBS)) {
      builder = builder.executors(FINISHED_JOBS.get(0).getExecutors());
    }

    return builder.build();
  }

  @AfterClass
  public static void closeAllJobs() {
    FINISHED_JOBS.forEach(TestJob::close);
    FINISHED_JOBS.clear();
  }

  /**
   * @param jobConf Job conf to run.
   */
  protected void submitJob(BitSailConfiguration jobConf,
                                  String jobName,
                                  int timeout,
                                  Consumer<AbstractDataSource> validation) throws Exception {
    TestJob testJob = createJob(jobConf);
    int exitCode;

    try {
      exitCode = testJob.run(jobName, timeout);
      if (exitCode != 0) {
        throw new IllegalStateException("Failed to execute job with exit code " + exitCode);
      }

      testJob.validate(validation);
    } catch (Throwable t) {
      LOG.error("Job failed.", t);
      throw t;
    } finally {
      if (!reuseExecutor) {
        testJob.closeExecutors();
      }
      if (!reuseSource) {
        testJob.closeSource();
      }
      if (!reuseSink) {
        testJob.closeSink();
      }
      if (reuseExecutor || reuseSource || reuseSink) {
        FINISHED_JOBS.add(testJob);
      }
    }
  }

  protected void submitJob(BitSailConfiguration jobConf,
                                  String jobName) throws Exception {
    submitJob(jobConf, jobName, 0, null);
  }

  protected void submitJob(BitSailConfiguration jobConf,
                                  String jobName,
                                  int timeout) throws Exception {
    submitJob(jobConf, jobName, timeout, null);
  }

  protected void submitJob(BitSailConfiguration jobConf,
                                  String jobName,
                                  Consumer<AbstractDataSource> validation) throws Exception {
    submitJob(jobConf, jobName, 0, validation);
  }
}
