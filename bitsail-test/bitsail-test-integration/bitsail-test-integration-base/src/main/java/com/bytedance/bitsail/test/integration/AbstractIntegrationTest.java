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

package com.bytedance.bitsail.test.integration;

import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.option.CommonOptions;
import com.bytedance.bitsail.common.util.Pair;
import com.bytedance.bitsail.test.integration.engine.EngineType;
import com.bytedance.bitsail.test.integration.engine.IntegrationEngine;
import com.bytedance.bitsail.test.integration.error.IntegrationTestErrorCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * Note: If you extend  {@link AbstractIntegrationTest}, the class name should end with "ITCase".
 */
@AbstractIntegrationTest.RunWith(with = {EngineType.FLINK_1_11, EngineType.FLINK_1_16})
public abstract class AbstractIntegrationTest {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractIntegrationTest.class);

  private static final String INTEGRATION_TEST_CLASS_NAME_PREFIX = "ITCase";

  protected List<EngineType> engineTypes;

  protected boolean exitUponException = false;

  protected AbstractIntegrationTest() {
    // Check if the class name is ended with "ITCase".
    String className = getClass().getSimpleName();
    if (!className.endsWith(INTEGRATION_TEST_CLASS_NAME_PREFIX)) {
      throw BitSailException.asBitSailException(IntegrationTestErrorCode.INTEGRATION_CLASS_NAME_ERROR,
          "Current test class name is: " + getClass().getName());
    }

    engineTypes = new ArrayList<>();

    RunWith runWithEngines = getClass().getAnnotation(RunWith.class);
    if (runWithEngines != null) {
      engineTypes.addAll(Arrays.asList(runWithEngines.with()));
    }

    runWithEngines = AbstractIntegrationTest.class.getAnnotation(RunWith.class);
    if (runWithEngines != null) {
      engineTypes.addAll(Arrays.asList(runWithEngines.with()));
    }
  }

  public void submitJob(BitSailConfiguration jobConf) throws Exception {
    String jobName = jobConf.getUnNecessaryOption(CommonOptions.JOB_NAME, "default_integration_job_name");

    List<Pair<EngineType, Throwable>> throwableList = new ArrayList<>();

    for (EngineType engineType : engineTypes) {
      IntegrationEngine engine = engineType.getInstance();
      if (engine.available()) {
        try {
          LOG.info("Running test [{}] on engine [{}].", jobName, engineType.name());
          engine.submitJob(jobConf);
        } catch (Throwable t) {
          LOG.error("Integration test [{}] Failed on engine [{}].", jobName, engineType.name(), t);
          if (exitUponException) {
            throw t;
          }
          throwableList.add(Pair.newPair(engineType, t));
        }
      }
    }

    if (!throwableList.isEmpty()) {
      throw BitSailException.asBitSailException(IntegrationTestErrorCode.INTEGRATION_TEST_FAILED,
          String.format("Integration test [%s] failed on [%d] engines: [%s], please check the error log.",
              jobName,
              throwableList.size(),
              throwableList.stream().map(p -> p.getFirst().name()).collect(Collectors.joining(", ")))
      );
    }
  }

  /**
   * Submit the job with execution timeout. For testing streaming job.
   * @param jobConf
   * @param execTimeout
   * @throws Exception
   */
  public void submitJob(BitSailConfiguration jobConf, int execTimeout) throws Exception {
    int exitCode;
    ExecutorService service = Executors.newSingleThreadExecutor();
    try {
      Future<?> future = service.submit(
          () -> {
            try {
                submitJob(jobConf);
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          });
      future.get(execTimeout, TimeUnit.SECONDS);
    } catch (TimeoutException te) {
      LOG.info("Execute more than {} seconds, will terminate it.", execTimeout);
    } catch (Exception e) {
      LOG.error("Failed to execute job.", e);
      throw e;
    }
  }

  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.TYPE)
  public @interface RunWith {
    EngineType[] with() default {};
  }
}
