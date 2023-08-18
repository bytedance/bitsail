/*
 *       Copyright 2022-2023 Bytedance Ltd. and/or its affiliates.
 *
 *       Licensed under the Apache License, Version 2.0 (the "License");
 *       you may not use this file except in compliance with the License.
 *       You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *       Unless required by applicable law or agreed to in writing, software
 *       distributed under the License is distributed on an "AS IS" BASIS,
 *       WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *       See the License for the specific language governing permissions and
 *       limitations under the License.
 */

package com.bytedance.bitsail.connector.elasticsearch.utils;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.connector.elasticsearch.constants.Elasticsearchs;
import com.bytedance.bitsail.connector.elasticsearch.option.ElasticsearchOptions;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class ElasticsearchUtils {

  public static RestClientBuilder prepareRestClientBuilder(BitSailConfiguration configuration) {
    String hosts = configuration.get(ElasticsearchOptions.HOSTS);

    List<HttpHost> httpHosts = Arrays.stream(StringUtils.split(hosts, Elasticsearchs.COMMA))
        .map(HttpHost::create)
        .collect(Collectors.toList());
    RestClientBuilder builder = RestClient.builder(httpHosts.toArray(new HttpHost[] {}));

    String username = configuration.get(ElasticsearchOptions.USERNAME);
    String password = configuration.get(ElasticsearchOptions.PASSWORD);
    if (StringUtils.isNotEmpty(username)
        || StringUtils.isNotEmpty(password)) {

      CredentialsProvider provider = new BasicCredentialsProvider();
      provider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
      builder.setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(provider));
    }

    String pathPrefix = configuration.get(ElasticsearchOptions.PATH_PREFIX);
    if (StringUtils.isNotEmpty(pathPrefix)) {
      builder.setPathPrefix(pathPrefix);
    }

    int connectionRequestTimeout = configuration.get(ElasticsearchOptions.CONNECTION_REQUEST_TIMEOUT_MS);
    int connectionTimeout = configuration.get(ElasticsearchOptions.CONNECTION_TIMEOUT_MS);
    int socketTimeout = configuration.get(ElasticsearchOptions.SOCKET_TIMEOUT_MS);

    builder.setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder
        .setConnectionRequestTimeout(connectionRequestTimeout)
        .setConnectTimeout(connectionTimeout)
        .setSocketTimeout(socketTimeout));

    return builder;
  }
}
