/*
 * Copyright 2022 Bytedance Ltd. and/or its affiliates.
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

package com.bytedance.bitsail.connector.elasticsearch;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.connector.elasticsearch.option.ElasticsearchReaderOptions;
import com.bytedance.bitsail.connector.elasticsearch.option.ElasticsearchWriterOptions;
import com.bytedance.bitsail.connector.elasticsearch.util.SourceSetupUtil;
import com.bytedance.bitsail.test.connector.test.EmbeddedFlinkCluster;
import com.bytedance.bitsail.test.connector.test.testcontainers.elasticsearch.ElasticsearchCluster;
import com.bytedance.bitsail.test.connector.test.utils.JobConfUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

public class ElasticsearchSourceITCase {

  private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchSourceITCase.class);

  private ElasticsearchCluster esCluster;

  private final String indices = "test1, test2";

  private final int count = 10;

  private SourceSetupUtil sourceEnv;

  @Before
  public void prepareEsCluster() throws Exception {
    esCluster = new ElasticsearchCluster();
    esCluster.startService();
    esCluster.checkClusterHealth();

    BitSailConfiguration jobConf = BitSailConfiguration.newDefault();
    jobConf.set(ElasticsearchWriterOptions.ES_HOSTS,
        Collections.singletonList(esCluster.getHttpHostAddress()));

    this.sourceEnv =
        SourceSetupUtil.builder()
            .esCluster(esCluster)
            .jobConf(jobConf)
            .indices(Arrays.asList("test1", "test2"))
            .count(count)
            .build();

    sourceEnv.start();
  }

  @Test
  public void testSource() throws Exception {
    BitSailConfiguration jobConf = JobConfUtils.fromClasspath("es_source_test.json");

    jobConf.set(ElasticsearchReaderOptions.ES_INDEX, indices);
    jobConf.set(ElasticsearchReaderOptions.ES_HOSTS,
        Collections.singletonList(esCluster.getHttpHostAddress()));
    jobConf.set(ElasticsearchReaderOptions.SCROLL_SIZE, 3);
    jobConf.set(ElasticsearchReaderOptions.SCROLL_TIME, "1m");

    EmbeddedFlinkCluster.submitJob(jobConf);
  }

  @After
  public void closeEsCluster() throws IOException {
    this.sourceEnv.client.close();
    esCluster.close();
  }
}
