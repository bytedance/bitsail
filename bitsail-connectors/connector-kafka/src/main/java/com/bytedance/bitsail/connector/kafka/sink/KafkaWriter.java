/*
 * Copyright 2022-2023 Bytedance Ltd. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.bytedance.bitsail.connector.kafka.sink;

import com.bytedance.bitsail.base.connector.writer.v1.Writer;
import com.bytedance.bitsail.base.connector.writer.v1.state.EmptyState;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.common.typeinfo.RowTypeInfo;
import com.bytedance.bitsail.connector.kafka.common.KafkaErrorCode;
import com.bytedance.bitsail.connector.kafka.discoverer.FixedTopicPartitionDiscoverer;
import com.bytedance.bitsail.connector.kafka.discoverer.PartitionDiscoverer;
import com.bytedance.bitsail.connector.kafka.format.ProducerRecordRowSerializationSchema;
import com.bytedance.bitsail.connector.kafka.format.ProducerRecordSerializationSchemaFactory;
import com.bytedance.bitsail.connector.kafka.option.KafkaOptions;
import com.bytedance.bitsail.connector.kafka.sink.callback.CallbackWrapper;

import org.apache.commons.collections.MapUtils;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

public class KafkaWriter<CommitT> implements Writer<Row, CommitT, EmptyState> {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaWriter.class);

  private final Properties properties;
  private final KafkaSender sender;
  private final RowTypeInfo rowTypeInfo;
  private final ProducerRecordRowSerializationSchema<byte[], byte[]> serializationSchema;
  private final CallbackWrapper callbackWrapper;
  private final PartitionDiscoverer partitionDiscoverer;
  private final transient Writer.Context<EmptyState> context;

  public KafkaWriter(BitSailConfiguration commonConfiguration, BitSailConfiguration writerConf, Context<EmptyState> context) {
    this.context = context;
    this.rowTypeInfo = context.getRowTypeInfo();
    this.properties = prepareSenderProperties(writerConf);
    this.sender = new KafkaSender(properties);
    this.partitionDiscoverer = new FixedTopicPartitionDiscoverer(sender.partitionFor(writerConf.get(KafkaOptions.TOPIC_NAME)));
    this.serializationSchema = ProducerRecordSerializationSchemaFactory.getRowSerializationSchema(writerConf, rowTypeInfo, partitionDiscoverer);
    this.callbackWrapper = new CallbackWrapper(writerConf, context);
  }

  protected Properties prepareSenderProperties(BitSailConfiguration jobConf) {
    Properties properties = new Properties();
    Map<String, String> optionalProperties = jobConf.get(KafkaOptions.PROPERTIES);
    if (MapUtils.isNotEmpty(optionalProperties)) {
      properties.putAll(optionalProperties);
    }

    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
        jobConf.getNecessaryOption(KafkaOptions.BOOTSTRAP_SERVERS, KafkaErrorCode.REQUIRED_VALUE));
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        ByteArraySerializer.class.getName());
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        ByteArraySerializer.class.getName());

    if (!properties.contains(ProducerConfig.RETRIES_CONFIG)) {
      properties.put(ProducerConfig.RETRIES_CONFIG, jobConf.get(KafkaOptions.RETRIES));
    }

    if (!properties.contains(ProducerConfig.RETRY_BACKOFF_MS_CONFIG)) {
      properties.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, jobConf.get(KafkaOptions.RETRY_BACKOFF_MS));
    }

    if (!properties.contains(ProducerConfig.LINGER_MS_CONFIG)) {
      properties.put(ProducerConfig.LINGER_MS_CONFIG, jobConf.get(KafkaOptions.LINGER_MS));
    }
    return properties;
  }

  @Override
  public void write(Row record) throws IOException {
    callbackWrapper.checkErroneous();
    ProducerRecord<byte[], byte[]> producerRecord = serializationSchema.serialize(record);
    sender.send(producerRecord, callbackWrapper);
  }

  @Override
  public void flush(boolean endOfInput) throws IOException {
    sender.flush();
  }

  @Override
  public List<CommitT> prepareCommit() throws IOException {
    return Collections.emptyList();
  }

  @Override
  public List<EmptyState> snapshotState(long checkpointId) throws IOException {
    return Collections.emptyList();
  }

  @Override
  public void close() throws IOException {
    if (Objects.nonNull(partitionDiscoverer)) {
      partitionDiscoverer.close();
    }

    if (Objects.nonNull(sender)) {
      this.sender.close();
    }
  }
}
