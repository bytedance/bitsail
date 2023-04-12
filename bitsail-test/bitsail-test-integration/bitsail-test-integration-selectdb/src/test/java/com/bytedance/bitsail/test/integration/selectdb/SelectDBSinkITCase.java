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

package com.bytedance.bitsail.test.integration.selectdb;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.option.CommonOptions;
import com.bytedance.bitsail.connector.selectdb.config.SelectdbWriterOptions;
import com.bytedance.bitsail.test.integration.AbstractIntegrationTest;
import com.bytedance.bitsail.test.integration.utils.JobConfUtils;

import org.junit.Ignore;
import org.junit.Test;

@Ignore("Ignore tests which cannot offer local test data sources.")
public class SelectDBSinkITCase extends AbstractIntegrationTest {

  @Test
  public void test() throws Exception {
    BitSailConfiguration jobConf = JobConfUtils.fromClasspath("fake_to_selectdb.json");
    addSelectdbInfo(jobConf);
    submitJob(jobConf);
  }

  /**
   * Add your selectdb setting to job configuration.
   * Below codes are just example.
   */
  public void addSelectdbInfo(BitSailConfiguration jobConf) {
    jobConf.set(SelectdbWriterOptions.LOAD_URL, "<selectdb url>:<http port>");
    jobConf.set(SelectdbWriterOptions.JDBC_URL, "<selectdb url>:<mysql port>");
    jobConf.set(SelectdbWriterOptions.CLUSTER_NAME, "test_cluster");
    jobConf.set(SelectdbWriterOptions.USER, "admin");
    jobConf.set(SelectdbWriterOptions.PASSWORD, "password");
    jobConf.set(SelectdbWriterOptions.TABLE_IDENTIFIER, "test.test_bitsail");
    jobConf.set(SelectdbWriterOptions.SINK_LABEL_PREFIX, "bitsail-selectdb");
    jobConf.set(CommonOptions.CheckPointOptions.CHECKPOINT_ENABLE, true);
    jobConf.set(CommonOptions.CheckPointOptions.CHECKPOINT_INTERVAL, 5000L);
  }
}
