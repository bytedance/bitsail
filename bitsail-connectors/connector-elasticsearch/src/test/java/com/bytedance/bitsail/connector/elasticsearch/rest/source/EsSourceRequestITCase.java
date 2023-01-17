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

package com.bytedance.bitsail.connector.elasticsearch.rest.source;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.connector.elasticsearch.option.ElasticsearchWriterOptions;
import com.bytedance.bitsail.connector.elasticsearch.utils.SourceSetupUtils;
import com.bytedance.bitsail.test.connector.test.testcontainers.elasticsearch.ElasticsearchCluster;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class EsSourceRequestITCase {

  private static final Logger LOG = LoggerFactory.getLogger(EsSourceRequestITCase.class);
  private SourceSetupUtils sourceEnv;

  private ElasticsearchCluster esCluster;

  private EsSourceRequest esSourceRequest;

  private final String index = "test1";

  private final int count = 10;

  @Before
  public void setup() throws IOException, InterruptedException {
    esCluster = new ElasticsearchCluster();
    esCluster.startService();
    esCluster.checkClusterHealth();

    BitSailConfiguration jobConf = BitSailConfiguration.newDefault();
    jobConf.set(ElasticsearchWriterOptions.ES_HOSTS,
        Collections.singletonList(esCluster.getHttpHostAddress()));

    this.sourceEnv =
        SourceSetupUtils.builder()
            .esCluster(esCluster)
            .jobConf(jobConf)
            .indices(Collections.singletonList(index))
            .count(count)
            .build();

    sourceEnv.start();
    esSourceRequest = new EsSourceRequest(this.sourceEnv.client);
  }

  @Test
  public void testGetAllDocuments() throws IOException {
    List<Map<String, Object>> allDocuments = esSourceRequest.getAllDocuments(index,
        Arrays.asList("id", "text_type", "keyword_type", "long_type", "date_type"),
        3, "30s");
    LOG.info("All documents: {}", allDocuments);
    assertEquals("Get All Document through scroll api wrong.", count, allDocuments.size());
  }

  @Test
  public void testValidateIndex() {
    Long getCount = esSourceRequest.validateIndex(index);
    assertEquals("Document count don't match.", count, getCount.intValue());
  }

  @After
  public void tearDown() throws IOException {
    this.sourceEnv.client.close();
    this.esCluster.close();
  }
}
