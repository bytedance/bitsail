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

package com.bytedance.bitsail.test.e2e.datasource;

import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.exception.CommonErrorCode;
import com.bytedance.bitsail.common.option.WriterOptions;
import com.bytedance.bitsail.connector.elasticsearch.option.ElasticsearchOptions;
import com.bytedance.bitsail.connector.elasticsearch.sink.ElasticsearchSink;

import lombok.SneakyThrows;
import org.apache.http.HttpHost;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;

public class ElasticsearchDataSource extends AbstractDataSource {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractDataSource.class);

  private static final String ELASTICSEARCH_VERSION = "7.10.2";
  private static final DockerImageName ELASTICSEARCH_IMAGE = DockerImageName
      .parse("docker.elastic.co/elasticsearch/elasticsearch")
      .withTag(ELASTICSEARCH_VERSION);

  private static final String ES_INDEX = "es_index_test";

  private ElasticsearchContainer esContainer;

  /**
   * Connection info.
   */
  private String hostAlias;
  private static final int REQUEST_PORT = 9200;

  @Override
  public String getContainerName() {
    return "data-source-elasticsearch";
  }

  @Override
  public void initNetwork(Network executorNetwork) {
    this.network = executorNetwork;
  }

  @Override
  public void configure(BitSailConfiguration dataSourceConf) {
    // nothing to do
  }

  @Override
  public boolean accept(BitSailConfiguration jobConf, Role role) {
    String writerClass = jobConf.get(WriterOptions.WRITER_CLASS);

    return role == Role.SINK
        && ElasticsearchSink.class.getName().equals(writerClass);
  }

  @Override
  public void modifyJobConf(BitSailConfiguration jobConf) {
    jobConf.setWriter(ElasticsearchOptions.HOSTS,
        getInternalHttpHostAddress());
    jobConf.setWriter(ElasticsearchOptions.INDEX, ES_INDEX);
  }

  @SuppressWarnings("checkstyle:MagicNumber")
  @Override
  public void start() {
    hostAlias = getContainerName() + "-" + role;

    // start service
    esContainer = new ElasticsearchContainer(ELASTICSEARCH_IMAGE)
        .withNetwork(network)
        .withNetworkAliases(hostAlias)
        .withEnv("bootstrap.system_call_filter", "false")
        .withStartupTimeout(Duration.of(3, ChronoUnit.MINUTES));
    esContainer.start();
    checkClusterHealth();

    // create index
    createIndex(ES_INDEX);

    LOG.info("Elasticsearch container starts! Host is: [{}].", esContainer.getHttpHostAddress());
  }

  @Override
  public void reset() {
    deleteIndex(ES_INDEX);
    createIndex(ES_INDEX);
  }

  @Override
  public void close() throws IOException {
    esContainer.close();
    super.close();
  }

  /**
   * Get internal host address for executor.
   */
  protected String getInternalHttpHostAddress() {
    return hostAlias + ":" + REQUEST_PORT;
  }

  /**
   * Check if es container is working.
   */
  @SuppressWarnings("checkstyle:MagicNumber")
  @SneakyThrows
  protected void checkClusterHealth() {
    RestClientBuilder builder = getRestClientBuilder();
    RestClient client = builder.build();

    Response response = client.performRequest(new Request("GET", "/_cluster/health"));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    Assert.assertTrue(EntityUtils.toString(response.getEntity()).contains("cluster_name"));
  }

  /**
   * Create an index.
   */
  @SneakyThrows
  public void createIndex(String indexName) {
    RestClientBuilder builder = getRestClientBuilder();
    RestHighLevelClient client = new RestHighLevelClient(builder);

    client.indices().create(new CreateIndexRequest(indexName), RequestOptions.DEFAULT);
  }

  /**
   * Delete an index.
   */
  public void deleteIndex(String indexName) {
    RestClientBuilder builder = getRestClientBuilder();
    RestHighLevelClient client = new RestHighLevelClient(builder);

    try {
      DeleteIndexRequest request = new DeleteIndexRequest(indexName);
      client.indices().delete(request, RequestOptions.DEFAULT);
    } catch (Exception ignored) {
      // ignored
    }
  }

  /**
   * Get doc count in ES_INDEX.
   */
  public long getCount() {
    try {
      RestClientBuilder builder = getRestClientBuilder();
      RestClient lowClient = builder.build();
      lowClient.performRequest(new Request("POST", "/_flush"));
      LOG.info("Flush all indices in cluster.");

      RestHighLevelClient client = new RestHighLevelClient(builder);
      CountRequest countRequest = new CountRequest(ES_INDEX);
      CountResponse countResponse = client.count(countRequest, RequestOptions.DEFAULT);
      LOG.info("Found {} docs.", countResponse.getCount());
      return countResponse.getCount();
    } catch (IOException e) {
      throw BitSailException.asBitSailException(CommonErrorCode.RUNTIME_ERROR,
          "Failed to count docs from es cluster.", e);
    }
  }

  /**
   * Create a rest client builder.
   */
  private RestClientBuilder getRestClientBuilder() {
    return RestClient.builder(HttpHost.create(esContainer.getHttpHostAddress()));
  }
}
