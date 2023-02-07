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

package com.bytedance.bitsail.connector.kudu.sink;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.connector.fake.option.FakeReaderOptions;
import com.bytedance.bitsail.connector.kudu.KuduTestUtils;
import com.bytedance.bitsail.connector.kudu.option.KuduWriterOptions;
import com.bytedance.bitsail.test.connector.test.EmbeddedFlinkCluster;
import com.bytedance.bitsail.test.connector.test.utils.JobConfUtils;

import org.apache.kudu.client.KuduClient;
import org.apache.kudu.test.KuduTestHarness;
import org.apache.kudu.test.cluster.MiniKuduCluster;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class KuduWriterITCase {
  private static final String TABLE_NAME = "test_kudu_table";
  private static final int TOTAL_COUNT = 10000;

  /**
   * Note that the tablet server number should be larger than hash buckets number.
   */
  @Rule
  public KuduTestHarness harness = new KuduTestHarness(
      new MiniKuduCluster.MiniKuduClusterBuilder().numTabletServers(KuduTestUtils.BUCKET_NUM)
  );

  @Test
  public void testFakeToKudu() throws Exception {
    KuduClient client = harness.getClient();
    KuduTestUtils.createTable(client, TABLE_NAME);

    BitSailConfiguration jobConf = JobConfUtils.fromClasspath("fake_to_kudu.json");
    updateJobConf(jobConf);

    EmbeddedFlinkCluster.submitJob(jobConf);

    List<List<Object>> scanResults;
    try {
      scanResults = KuduTestUtils.scanTable(client, TABLE_NAME);
    } catch (Exception e) {
      throw new RuntimeException("Failed to scan rows from table " + TABLE_NAME, e);
    }

    // KuduTableStatistics::getLiveRowCount() is not accurate, so we count the scan results here.
    Assert.assertEquals(TOTAL_COUNT, scanResults.size());
  }

  private void updateJobConf(BitSailConfiguration jobConf) {
    jobConf.set(FakeReaderOptions.TOTAL_COUNT, TOTAL_COUNT);
    jobConf.set(FakeReaderOptions.RATE, TOTAL_COUNT / 5);
    jobConf.set(KuduWriterOptions.KUDU_TABLE_NAME, TABLE_NAME);

    String masterAddressString = harness.getMasterAddressesAsString();
    List<String> masterAddressList = Arrays.stream(masterAddressString.split(",")).collect(Collectors.toList());
    jobConf.set(KuduWriterOptions.MASTER_ADDRESS_LIST, masterAddressList);
  }
}
