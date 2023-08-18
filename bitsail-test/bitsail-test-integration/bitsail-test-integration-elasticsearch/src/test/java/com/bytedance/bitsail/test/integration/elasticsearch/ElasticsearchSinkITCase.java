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

package com.bytedance.bitsail.test.integration.elasticsearch;

import com.bytedance.bitsail.base.execution.Mode;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.option.CommonOptions;
import com.bytedance.bitsail.common.option.WriterOptions;
import com.bytedance.bitsail.connector.elasticsearch.option.ElasticsearchOptions;
import com.bytedance.bitsail.connector.elasticsearch.utils.ElasticsearchUtils;
import com.bytedance.bitsail.connector.fake.option.FakeReaderOptions;
import com.bytedance.bitsail.test.integration.AbstractIntegrationTest;
import com.bytedance.bitsail.test.integration.elasticsearch.container.ElasticsearchCluster;
import com.bytedance.bitsail.test.integration.utils.JobConfUtils;

import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

@SuppressWarnings("checkstyle:MagicNumber")
public class ElasticsearchSinkITCase extends AbstractIntegrationTest {

  private static final int TOTAL_COUNT = 300;
  private static final String INDEX = "es_index_test";
  private static ElasticsearchCluster cluster;

  private final CountRequest countRequest = new CountRequest(INDEX);
  private RestHighLevelClient client;

  @BeforeClass
  public static void beforeClass() throws Exception {
    cluster = new ElasticsearchCluster();
    cluster.startService();
    cluster.checkClusterHealth();
  }

  @Before
  public void before() {
    cluster.resetIndex(INDEX);
    BitSailConfiguration jobConf = BitSailConfiguration.newDefault();
    jobConf.setWriter(ElasticsearchOptions.HOSTS,
        cluster.getHttpHostAddress());
    client = new RestHighLevelClient(ElasticsearchUtils
        .prepareRestClientBuilder(jobConf.getSubConfiguration(WriterOptions.JOB_WRITER)));
  }

  @After
  public void after() throws Exception {
    client.close();
  }

  @AfterClass
  public static void afterClass() {
    cluster.close();
  }

  @Test
  public void testBatch() throws Exception {
    BitSailConfiguration jobConf = JobConfUtils.fromClasspath("es_sink_test.json");

    jobConf.set(FakeReaderOptions.TOTAL_COUNT, TOTAL_COUNT);
    jobConf.set(FakeReaderOptions.RATE, 1000);
    jobConf.setWriter(ElasticsearchOptions.INDEX, INDEX);
    jobConf.setWriter(ElasticsearchOptions.HOSTS, cluster.getHttpHostAddress());

    submitJob(jobConf);

    cluster.flush();
    CountResponse countResponse = client.count(countRequest, RequestOptions.DEFAULT);
    Assert.assertEquals(TOTAL_COUNT, countResponse.getCount());
  }

  @Test
  public void testStreaming() throws Exception {
    BitSailConfiguration jobConf = JobConfUtils.fromClasspath("es_sink_test.json");

    jobConf.set(CommonOptions.JOB_TYPE, Mode.STREAMING.name());
    jobConf.set(CommonOptions.CheckPointOptions.CHECKPOINT_ENABLE, true);
    jobConf.set(FakeReaderOptions.TOTAL_COUNT, TOTAL_COUNT);
    jobConf.set(FakeReaderOptions.RATE, TOTAL_COUNT / 10);

    jobConf.setWriter(ElasticsearchOptions.INDEX, INDEX);
    jobConf.setWriter(ElasticsearchOptions.HOSTS, cluster.getHttpHostAddress());

    submitJob(jobConf);

    cluster.flush();
    CountResponse countResponse = client.count(countRequest, RequestOptions.DEFAULT);
    Assert.assertEquals(TOTAL_COUNT, countResponse.getCount());
  }
}
