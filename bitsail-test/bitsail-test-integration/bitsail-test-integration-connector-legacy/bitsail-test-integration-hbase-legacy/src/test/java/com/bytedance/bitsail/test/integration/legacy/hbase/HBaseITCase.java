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

package com.bytedance.bitsail.test.integration.legacy.hbase;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.connector.hbase.option.HBaseReaderOptions;
import com.bytedance.bitsail.connector.hbase.option.HBaseWriterOptions;
import com.bytedance.bitsail.test.integration.AbstractIntegrationTest;
import com.bytedance.bitsail.test.integration.legacy.hbase.container.HbaseCluster;
import com.bytedance.bitsail.test.integration.utils.JobConfUtils;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;

public class HBaseITCase extends AbstractIntegrationTest {
  private static final String SOURCE_TABLE = "test_table_source";
  private static final String SINK_TABLE = "test_table_sink";

  private static HbaseCluster hbaseCluster;

  @BeforeClass
  public static void prepareCluster() {
    hbaseCluster = new HbaseCluster();
    hbaseCluster.startService();
  }

  @AfterClass
  public static void closeCluster() {
    hbaseCluster.stopService();
  }

  @Test
  public void testHbaseToPrint() throws Exception {
    hbaseCluster.createTable(SOURCE_TABLE, Arrays.asList("cf1", "cf2", "cf3"));
    hbaseCluster.putRow(SOURCE_TABLE, "row1", "cf1", "int1", "10");
    hbaseCluster.putRow(SOURCE_TABLE, "row1", "cf1", "str1", "aaaaa");
    hbaseCluster.putRow(SOURCE_TABLE, "row1", "cf2", "int2", "0");
    hbaseCluster.putRow(SOURCE_TABLE, "row1", "cf3", "str2", "ccccc");
    hbaseCluster.putRow(SOURCE_TABLE, "row2", "cf1", "int1", "10");
    hbaseCluster.putRow(SOURCE_TABLE, "row2", "cf1", "str1", "bbbbb");
    hbaseCluster.putRow(SOURCE_TABLE, "row2", "cf2", "int2", "0");
    hbaseCluster.putRow(SOURCE_TABLE, "row2", "cf3", "str2", "ddddd");

    BitSailConfiguration conf = JobConfUtils.fromClasspath("hbase_to_print.json");
    conf.set(HBaseReaderOptions.TABLE, SOURCE_TABLE);
    submitJob(conf);
  }

  @Test
  public void testFakeToHBase() throws Exception {
    hbaseCluster.createTable(SINK_TABLE, Arrays.asList("cf1", "cf2", "cf3"));

    BitSailConfiguration conf = JobConfUtils.fromClasspath("fake_to_hbase.json");
    conf.set(HBaseWriterOptions.TABLE_NAME, SINK_TABLE);
    submitJob(conf);
  }
}
