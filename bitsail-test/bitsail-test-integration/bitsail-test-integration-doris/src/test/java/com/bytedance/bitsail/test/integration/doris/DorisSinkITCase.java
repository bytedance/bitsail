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

package com.bytedance.bitsail.test.integration.doris;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.option.CommonOptions;
import com.bytedance.bitsail.connector.doris.option.DorisWriterOptions;
import com.bytedance.bitsail.connector.fake.option.FakeReaderOptions;
import com.bytedance.bitsail.test.integration.AbstractIntegrationTest;
import com.bytedance.bitsail.test.integration.utils.JobConfUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Map;

/**
 * todo: add doris data source for integration test.
 */
@Ignore
public class DorisSinkITCase extends AbstractIntegrationTest {

  @Test
  public void test() throws Exception {
    BitSailConfiguration jobConf = JobConfUtils.fromClasspath("fake_to_doris.json");
    addDorisInfo(jobConf);
    submitJob(jobConf);
  }

  /**
   * Add your doris setting to job configuration.
   * Below codes are just example.
   */
  public void addDorisInfo(BitSailConfiguration jobConf) {
    jobConf.set(DorisWriterOptions.FE_HOSTS, "127.0.0.1:1234");
    jobConf.set(DorisWriterOptions.MYSQL_HOSTS, "127.0.0.1:4321");
    jobConf.set(DorisWriterOptions.USER, "test_user");
    jobConf.set(DorisWriterOptions.PASSWORD, "password");
    jobConf.set(DorisWriterOptions.DB_NAME, "test_db");
    jobConf.set(DorisWriterOptions.TABLE_NAME, "test_table");
    jobConf.set(CommonOptions.CheckPointOptions.CHECKPOINT_ENABLE, true);
    jobConf.set(CommonOptions.CheckPointOptions.CHECKPOINT_INTERVAL, 5000L);
    jobConf.set(DorisWriterOptions.SINK_ENABLE_2PC, false);
    jobConf.set(DorisWriterOptions.SINK_LABEL_PREFIX, "bitsail-doris");

    Map<String, Object> dorisPartition = ImmutableMap.of(
        "name", "p20221010",
        "start_range", ImmutableList.of("2022-10-10"),
        "end_range", ImmutableList.of("2022-10-11")
    );
    jobConf.set(DorisWriterOptions.PARTITIONS, ImmutableList.of(dorisPartition));
    jobConf.set(FakeReaderOptions.FROM_TIMESTAMP, "2022-10-10 01:00:00");
    jobConf.set(FakeReaderOptions.TO_TIMESTAMP, "2022-10-10 02:00:00");
  }
}
