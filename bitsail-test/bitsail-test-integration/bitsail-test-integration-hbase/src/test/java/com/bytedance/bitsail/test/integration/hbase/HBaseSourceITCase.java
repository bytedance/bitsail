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

package com.bytedance.bitsail.test.integration.hbase;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.test.integration.AbstractIntegrationTest;
import com.bytedance.bitsail.test.integration.hbase.container.HbaseCluster;
import com.bytedance.bitsail.test.integration.utils.JobConfUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

public class HBaseSourceITCase extends AbstractIntegrationTest {

  private HbaseCluster hbaseCluster;

  @Before
  public void initHbase() {
    hbaseCluster = new HbaseCluster();
    hbaseCluster.startService();
    hbaseCluster.createTable("test_table", Arrays.asList("cf1", "cf2", "cf3"));
    hbaseCluster.putRow("test_table", "row1", "cf1", "int1", "10");
    hbaseCluster.putRow("test_table", "row1", "cf1", "str1", "aaaaa");
    hbaseCluster.putRow("test_table", "row1", "cf2", "int2", "0");
    hbaseCluster.putRow("test_table", "row1", "cf3", "str2", "ccccc");
    hbaseCluster.putRow("test_table", "row2", "cf1", "int1", "10");
    hbaseCluster.putRow("test_table", "row2", "cf1", "str1", "bbbbb");
    hbaseCluster.putRow("test_table", "row2", "cf2", "int2", "0");
    hbaseCluster.putRow("test_table", "row2", "cf3", "str2", "ddddd");
  }

  @Test
  public void testFakeToHBase() throws Exception {
    BitSailConfiguration conf = JobConfUtils.fromClasspath("hbase_to_print.json");
    submitJob(conf);
  }

  @After
  public void closeHbase() {
    hbaseCluster.stopService();
  }

}
