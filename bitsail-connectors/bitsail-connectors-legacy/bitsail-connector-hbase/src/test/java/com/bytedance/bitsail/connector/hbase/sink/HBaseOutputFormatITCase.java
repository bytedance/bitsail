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

package com.bytedance.bitsail.connector.hbase.sink;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.test.connector.test.EmbeddedFlinkCluster;
import com.bytedance.bitsail.test.connector.test.testcontainers.hbase.HbaseCluster;
import com.bytedance.bitsail.test.connector.test.utils.JobConfUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;

@Ignore
public class HBaseOutputFormatITCase {

  private HbaseCluster hbaseCluster;

  @Before
  public void initHbase() {
    hbaseCluster = new HbaseCluster();
    hbaseCluster.startService();
    hbaseCluster.createTable("test_table", Arrays.asList("cf1", "cf2", "cf3"));
  }

  @Test
  public void testFakeToHBase() throws Exception {
    BitSailConfiguration conf = JobConfUtils.fromClasspath("fake_to_hbase.json");
    EmbeddedFlinkCluster.submitJob(conf);
  }

  @After
  public void closeHbase() {
    hbaseCluster.stopService();
  }
}
