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

package com.bytedance.bitsail.connector.elasticsearch.utils;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.connector.elasticsearch.rest.EsRestClientBuilder;
import com.bytedance.bitsail.test.connector.test.testcontainers.elasticsearch.ElasticsearchCluster;

import lombok.Builder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Builder
public class SourceSetupUtils {

  private static final Logger LOG = LoggerFactory.getLogger(SourceSetupUtils.class);

  private final String mappings = "{\n" +
      "  \"properties\": {\n" +
      "    \"id\": {\n" +
      "      \"type\": \"integer\"\n" +
      "    },\n" +
      "    \"text_type\": {\n" +
      "      \"type\": \"text\"\n" +
      "    },\n" +
      "    \"keyword_type\": {\n" +
      "      \"type\": \"keyword\"\n" +
      "    },\n" +
      "    \"long_type\": {\n" +
      "      \"type\": \"long\"\n" +
      "    },\n" +
      "    \"date_type\": {\n" +
      "      \"type\": \"date\"\n" +
      "    }\n" +
      "  }\n" +
      "}";

  public RestHighLevelClient client;

  public BitSailConfiguration jobConf;

  private ElasticsearchCluster esCluster;

  private int count;

  private List<String> indices;

  public void start() throws IOException, InterruptedException {
    client = new EsRestClientBuilder(jobConf).build();
    for (String index : indices) {
      createIndex(index);
      insertDocuments(index);
      TimeUnit.SECONDS.sleep(2);
      esCluster.flush();
      searchDocuments(index);
    }
  }

  @SuppressWarnings("checkstyle:MagicNumber")
  private void createIndex(String index) throws IOException {
    CreateIndexRequest request = new CreateIndexRequest(index);
    request.settings(Settings.builder()
        .put("index.number_of_shards", 3)
        .put("index.number_of_replicas", 0)
        .build());
    request.mapping(mappings, XContentType.JSON);
    CreateIndexResponse response = client.indices().create(request, RequestOptions.DEFAULT);
    assertTrue("Create index failed.", response.isShardsAcknowledged());
    LOG.info("Create {} index successfully. \n {}", response.index(), mappings);
  }

  @SuppressWarnings("checkstyle:MagicNumber")
  private void insertDocuments(String index) throws IOException {
    Map<String, Object> jsonMap = new HashMap<>();
    for (int i = 0; i < count; i++) {
      jsonMap.put("id", i);
      jsonMap.put("text_type", appendString("text", index, i));
      jsonMap.put("keyword_type", appendString("keyword", index, i));
      jsonMap.put("long_type", 20230116L + i);
      jsonMap.put("date_type", LocalDate.now());
      IndexRequest request = new IndexRequest(index).id(String.valueOf(i)).source(jsonMap);
      IndexResponse response = client.index(request, RequestOptions.DEFAULT);
      if (response.status() != RestStatus.CREATED) {
        LOG.error("Insert into index: {}, document {} failed", index, i);
        throw new RuntimeException("Insert document into index failed");
      }
    }
    LOG.info("Add {} documents to {} index successfully, mappings: ", count, index);
  }

  private String appendString(String type, String index, int idx) {
    return String.join("-", index, type, String.valueOf(idx));
  }

  private void searchDocuments(String index) throws IOException {
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.query(QueryBuilders.matchAllQuery());
    SearchRequest request = new SearchRequest();
    request.indices(index);
    request.source(searchSourceBuilder);
    SearchResponse response = client.search(request, RequestOptions.DEFAULT);
    SearchHits hits = response.getHits();
    LOG.info("Totally get {}", hits.getTotalHits());
    SearchHit[] searchHits = hits.getHits();
    assertEquals(String.format("Index: %s, doc count wrong.", index), count, searchHits.length);
    for (int i = 0; i < searchHits.length; i++) {
      LOG.info("Index: {}, hit-{}: {}", index, i, searchHits[i].getSourceAsString());
    }
  }
}
