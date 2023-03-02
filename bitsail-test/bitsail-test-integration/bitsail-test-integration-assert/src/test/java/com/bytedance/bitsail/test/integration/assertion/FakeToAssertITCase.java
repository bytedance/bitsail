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

package com.bytedance.bitsail.test.integration.assertion;

import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.test.integration.AbstractIntegrationTest;
import com.bytedance.bitsail.test.integration.utils.JobConfUtils;

import org.apache.flink.runtime.client.JobExecutionException;
import org.junit.Assert;
import org.junit.Test;

public class FakeToAssertITCase extends AbstractIntegrationTest {

  public FakeToAssertITCase() {
    super();
    this.exitUponException = true;
  }

  @Test
  public void testAssertWriter() throws Exception {
    BitSailConfiguration jobConf = JobConfUtils.fromClasspath("assert_sink.json");
    submitJob(jobConf);
  }

  @Test
  public void testAssertByColumnCheck() throws Exception {
    BitSailConfiguration jobConf = JobConfUtils.fromClasspath("assert_sink_col_check.json");
    BitSailException e = Assert.assertThrows(BitSailException.class, () -> submitJob(jobConf));
  }

  @Test
  public void testAssertByRowCheck() throws Exception {
    BitSailConfiguration jobConf = JobConfUtils.fromClasspath("assert_sink_row_check.json");
    JobExecutionException e = Assert.assertThrows(JobExecutionException.class, () -> submitJob(jobConf));
  }
}
