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

package com.bytedance.bitsail.connector.elasticsearch.sink;

import com.bytedance.bitsail.base.connector.writer.v1.Writer;
import com.bytedance.bitsail.base.connector.writer.v1.state.EmptyState;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.connector.elasticsearch.format.DefaultRowSerializationSchema;
import com.bytedance.bitsail.connector.elasticsearch.sink.listener.DefaultBulkListener;
import com.bytedance.bitsail.connector.elasticsearch.sink.sender.ElasticsearchSender;
import com.bytedance.bitsail.connector.elasticsearch.utils.ElasticsearchUtils;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class ElasticsearchWriter<CommitT> implements Writer<Row, CommitT, EmptyState> {
  private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchWriter.class);

  private final DefaultRowSerializationSchema serializationSchema;
  private final ElasticsearchSender elasticsearchSender;
  private final DefaultBulkListener bulkListener;
  private final RestHighLevelClient client;

  public ElasticsearchWriter(Context<EmptyState> context,
                             BitSailConfiguration commonConfiguration,
                             BitSailConfiguration writerConfiguration) {
    this.bulkListener = new DefaultBulkListener(context.getIndexOfSubTaskId());
    this.client = new RestHighLevelClient(ElasticsearchUtils.prepareRestClientBuilder(writerConfiguration));
    this.elasticsearchSender = new ElasticsearchSender(writerConfiguration, bulkListener, client);
    this.serializationSchema = new DefaultRowSerializationSchema(commonConfiguration,
        writerConfiguration,
        context.getRowTypeInfo());
  }

  @Override
  public void write(Row element) {
    bulkListener.checkErroneous();
    DocWriteRequest<?> serialize = serializationSchema.serialize(element);
    elasticsearchSender.bulkRequest(serialize);
  }

  @Override
  public void flush(boolean endOfInput) throws IOException {
    bulkListener.checkErroneous();
    elasticsearchSender.flush();
  }

  @Override
  public void close() throws IOException {
    elasticsearchSender.close();
    client.close();
    bulkListener.checkErroneous();
  }

  @Override
  public List<CommitT> prepareCommit() {
    bulkListener.checkErroneous();
    return Collections.emptyList();
  }

  @Override
  public List<EmptyState> snapshotState(long checkpointId) {
    bulkListener.checkErroneous();
    return Collections.emptyList();
  }
}
