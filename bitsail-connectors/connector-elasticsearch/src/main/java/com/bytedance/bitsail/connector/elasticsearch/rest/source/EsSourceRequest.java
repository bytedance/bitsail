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

import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.connector.elasticsearch.error.ElasticsearchErrorCode;

import org.apache.commons.compress.utils.Lists;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class EsSourceRequest {

  private static final Logger LOG = LoggerFactory.getLogger(EsSourceRequest.class);
  private final RestHighLevelClient restHighLevelClient;

  public EsSourceRequest(RestHighLevelClient restHighLevelClient) {
    this.restHighLevelClient = restHighLevelClient;
  }

  public Long validateIndex(String index) {
    CountRequest countRequest = new CountRequest(index);
    countRequest.query(QueryBuilders.matchAllQuery());
    CountResponse response = null;
    try {
      response = restHighLevelClient.count(countRequest, RequestOptions.DEFAULT);
      if (response == null) {
        throw new BitSailException(ElasticsearchErrorCode.VALID_INDEX_FAILED,
            "GET " + index + " metadata failed");
      }
      if (response.status() != RestStatus.OK) {
        throw new BitSailException(ElasticsearchErrorCode.VALID_INDEX_FAILED,
            String.format("Get %s response status code = %d", index, response.status().getStatus()));
      }
    } catch (IOException e) {
      throw new BitSailException(ElasticsearchErrorCode.VALID_INDEX_FAILED, e.getMessage());
    }
    return response.getCount();
  }

  /**
   * Returns a list of all records, Uses scroll API for pagination.
   * According to: <a href="https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/java-rest-high-search-scroll.html">...</a>
   *
   * @param index index name in Elasticsearch cluster
   * @param columnNames source field names
   * @param scrollSize scroll size
   * @param scrollTime scroll time
   * @return list of documents
   * @throws IOException throws IOException if Elasticsearch request fails
   */
  public List<Map<String, Object>> getAllDocuments(String index, List<String> columnNames, int scrollSize, String scrollTime) throws IOException {
    LOG.info("Start get all records from index: {}, scroll size: {}, scroll time: {}", index, scrollSize, scrollTime);
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.size(scrollSize);
    searchSourceBuilder.query(QueryBuilders.matchAllQuery());
    searchSourceBuilder.fetchSource(columnNames.toArray(new String[0]), null);
    searchSourceBuilder.sort("_doc");

    SearchRequest searchRequest = new SearchRequest(index);
    searchRequest.scroll(scrollTime);
    searchRequest.source(searchSourceBuilder);

    List<Map<String, Object>> allData = Lists.newArrayList();

    SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
    String scrollId = searchResponse.getScrollId();
    SearchHit[] hits = addSearchHits(searchResponse, allData);
    LOG.info("Finish first time search, get {} hits.", Objects.isNull(hits) ? 0 : hits.length);

    while (Objects.nonNull(hits) && hits.length > 0) {
      LOG.info("Continue scroll query with scrollId: {}", scrollId);
      SearchScrollRequest searchScrollRequest = new SearchScrollRequest(scrollId);
      searchScrollRequest.scroll(scrollTime);
      searchResponse = restHighLevelClient.scroll(searchScrollRequest, RequestOptions.DEFAULT);
      scrollId = searchResponse.getScrollId();
      hits = addSearchHits(searchResponse, allData);
    }

    clearScroll(scrollId);
    return allData;
  }

  private void clearScroll(String scrollId) throws IOException {
    ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
    clearScrollRequest.addScrollId(scrollId);
    ClearScrollResponse clearScrollResponse = restHighLevelClient.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
    boolean succeeded = clearScrollResponse.isSucceeded();
    if (succeeded) {
      LOG.info("Scroll response cleared successfully");
    } else {
      LOG.error("Fail to clear scroll response");
    }
  }

  private SearchHit[] addSearchHits(SearchResponse searchResponse, List<Map<String, Object>> allData) {
    SearchHit[] hits = searchResponse.getHits().getHits();
    if (Objects.isNull(hits)) {
      return null;
    }
    for (SearchHit hit : hits) {
      allData.add(hit.getSourceAsMap());
    }
    return hits;
  }
}
