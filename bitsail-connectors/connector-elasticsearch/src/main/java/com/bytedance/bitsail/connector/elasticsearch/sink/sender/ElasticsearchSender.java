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

package com.bytedance.bitsail.connector.elasticsearch.sink.sender;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.connector.elasticsearch.option.ElasticsearchOptions;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

public class ElasticsearchSender implements Serializable, Closeable {

  private final BitSailConfiguration configuration;

  private final BulkProcessor bulkProcessor;
  private final BulkProcessor.Listener listener;

  public ElasticsearchSender(BitSailConfiguration configuration,
                             BulkProcessor.Listener listener,
                             RestHighLevelClient client) {
    this.configuration = configuration;
    this.listener = listener;
    this.bulkProcessor = prepareBulkProcessor(configuration, client, listener);
  }

  private static BulkProcessor prepareBulkProcessor(BitSailConfiguration configuration,
                                                    RestHighLevelClient client,
                                                    BulkProcessor.Listener listener) {

    BiConsumer<BulkRequest, ActionListener<BulkResponse>> bulkConsumer =
        ((bulkRequest, bulkResponseActionListener) -> client
            .bulkAsync(bulkRequest, RequestOptions.DEFAULT, bulkResponseActionListener));

    BulkProcessor.Builder builder = BulkProcessor.builder(bulkConsumer, listener);
    //make flush blocking
    builder.setConcurrentRequests(0);

    Integer bulkMaxBatchCount = configuration.get(ElasticsearchOptions.BULK_MAX_BATCH_COUNT);
    Integer bulkMaxBatchSize = configuration.get(ElasticsearchOptions.BULK_MAX_BATCH_SIZE);
    Long flushInterval = configuration.get(ElasticsearchOptions.BULK_FLUSH_INTERVAL_MS);

    builder.setBulkActions(bulkMaxBatchCount)
        .setBulkSize(new ByteSizeValue(bulkMaxBatchSize, ByteSizeUnit.MB))
        .setFlushInterval(TimeValue.timeValueMillis(flushInterval))
        .setBackoffPolicy(
            BackoffPolicy.exponentialBackoff(
                new TimeValue(configuration.get(ElasticsearchOptions.BULK_BACKOFF_DELAY_MS),
                    TimeUnit.MILLISECONDS),
                configuration.get(ElasticsearchOptions.BULK_MAX_RETRY_COUNT))
        );

    return builder.build();
  }

  public void bulkRequest(DocWriteRequest<?> docWriteRequest) {
    bulkProcessor.add(docWriteRequest);
  }

  public void flush() {
    bulkProcessor.flush();
  }

  @Override
  public void close() throws IOException {
    if (Objects.nonNull(bulkProcessor)) {
      bulkProcessor.flush();
      bulkProcessor.close();
    }
  }
}
