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

package com.bytedance.bitsail.test.integration.kudu;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.connector.kudu.option.KuduReaderOptions;
import com.bytedance.bitsail.test.integration.AbstractIntegrationTest;
import com.bytedance.bitsail.test.integration.kudu.container.KuduDataSource;
import com.bytedance.bitsail.test.integration.utils.JobConfUtils;

import org.apache.kudu.client.KuduClient;
import org.apache.kudu.test.KuduTestHarness;
import org.apache.kudu.test.cluster.MiniKuduCluster;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Ignore("Mini kudu cluster cannot work on all platforms (e.g., windows)")
public class KuduSourceITCase extends AbstractIntegrationTest {

  private static final String TABLE_NAME = "test_kudu_table";
  private static final int TOTAL_COUNT = 1010;
  /**
   * Note that the tablet server number should be larger than hash buckets number.
   */
  @Rule
  public KuduTestHarness harness = new KuduTestHarness(
      new MiniKuduCluster.MiniKuduClusterBuilder().numTabletServers(KuduDataSource.BUCKET_NUM)
  );

  @Test
  public void testKuduToPrint() throws Exception {
    KuduClient client = harness.getClient();
    KuduDataSource.createTable(client, TABLE_NAME);
    KuduDataSource.insertRandomData(client, TABLE_NAME, TOTAL_COUNT);

    BitSailConfiguration jobConf = JobConfUtils.fromClasspath("kudu_to_print.json");
    updateJobConf(jobConf);
    submitJob(jobConf);
  }

  private void updateJobConf(BitSailConfiguration jobConf) {
    String masterAddressString = harness.getMasterAddressesAsString();
    List<String> masterAddressList = Arrays.stream(masterAddressString.split(",")).collect(Collectors.toList());
    jobConf.set(KuduReaderOptions.MASTER_ADDRESS_LIST, masterAddressList);
    jobConf.set(KuduReaderOptions.KUDU_TABLE_NAME, TABLE_NAME);
  }
}
