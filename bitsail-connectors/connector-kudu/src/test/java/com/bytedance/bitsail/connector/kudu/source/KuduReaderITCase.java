/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.bytedance.bitsail.connector.kudu.source;

import com.bytedance.bitsail.connector.kudu.KuduTestUtils;

import org.apache.kudu.client.KuduClient;
import org.apache.kudu.test.KuduTestHarness;
import org.apache.kudu.test.cluster.MiniKuduCluster;
import org.junit.Rule;
import org.junit.Test;

public class KuduReaderITCase {
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
  public void testKuduToPrint() throws Exception {
    KuduClient client = harness.getClient();
    KuduTestUtils.createTable(client, TABLE_NAME);
  }
}
