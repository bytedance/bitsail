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

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;

public class AbstractE2ETest {

  public static final String EMPTY_SOURCE = "empty";
  public static final String ENGINE_FLINK = "flink11";

  /**
   * @param jobConf Job conf to run.
   * @param engineType Executor engine type.
   * @param sourceType Reader data source type.
   * @param sinkType Writer data source type.
   * @return Exit code of test job.
   */
  public int submitJob(BitSailConfiguration jobConf,
                       String engineType,
                       String sourceType,
                       String sinkType) throws Exception {
    TestJob testJob = TestJob.builder()
        .withJobConf(jobConf)
        .withEngineType(engineType)
        .withSourceType(sourceType)
        .withSinkType(sinkType)
        .build();

    return testJob.run();
  }

  public int submitJob(BitSailConfiguration jobConf, String engineType) throws Exception {
    return submitJob(jobConf, engineType, EMPTY_SOURCE, EMPTY_SOURCE);
  }

  public int submitFlink11Job(BitSailConfiguration jobConf, String sourceType, String sinkType) throws Exception {
    return submitJob(jobConf, "flink11", sourceType, sinkType);
  }

  public int submitFlink11Job(BitSailConfiguration jobConf) throws Exception {
    return submitJob(jobConf, "flink11", EMPTY_SOURCE, EMPTY_SOURCE);
  }
}
