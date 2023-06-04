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

package com.bytedance.bitsail.connector.kafka.source.reader;

import com.bytedance.bitsail.base.connector.reader.v1.Boundedness;
import com.bytedance.bitsail.base.connector.reader.v1.SourcePipeline;
import com.bytedance.bitsail.base.connector.reader.v1.SourceReader;
import com.bytedance.bitsail.base.format.DeserializationSchema;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.connector.kafka.format.KafkaDeserializationSchema;
import com.bytedance.bitsail.connector.kafka.option.KafkaSourceOptions;
import com.bytedance.bitsail.connector.kafka.source.split.KafkaSplit;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

public class KafkaSourceReader implements SourceReader<Row, KafkaSplit> {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaSourceReader.class);

  private final String bootstrapServer;
  private final String topic;
  private final String consumerGroup;
  private final int pollBatchSize;
  private final long pollTimeout;
  private final Boundedness boundedness;
  private final boolean commitInCheckpoint;

  private final BitSailConfiguration readerConfiguration;
  private final transient Context context;
  private final transient Set<KafkaSplit> assignedKafkaSplits;
  private final transient Set<KafkaSplit> finishedKafkaSplits;
  private final transient DeserializationSchema<byte[], Row> deserializationSchema;
  private transient boolean noMoreSplits;
  private transient Consumer<byte[], byte[]> consumer;

  public KafkaSourceReader(BitSailConfiguration readerConfiguration,
                           Context context,
                           Boundedness boundedness) {
    this.readerConfiguration = readerConfiguration;
    this.boundedness = boundedness;
    this.context = context;
    this.assignedKafkaSplits = Sets.newHashSet();
    this.finishedKafkaSplits = Sets.newHashSet();
    this.deserializationSchema = new KafkaDeserializationSchema(
        readerConfiguration,
        context.getRowTypeInfo()
    );
    this.noMoreSplits = false;

    this.bootstrapServer = readerConfiguration.get(KafkaSourceOptions.BOOTSTRAP_SERVERS);
    this.topic = readerConfiguration.get(KafkaSourceOptions.TOPIC);
    this.consumerGroup = readerConfiguration.get(KafkaSourceOptions.CONSUMER_GROUP);
    this.pollBatchSize = readerConfiguration.get(KafkaSourceOptions.POLL_BATCH_SIZE);
    this.pollTimeout = readerConfiguration.get(KafkaSourceOptions.POLL_TIMEOUT);
    this.commitInCheckpoint = readerConfiguration.get(KafkaSourceOptions.COMMIT_IN_CHECKPOINT);
  }

  @Override
  public void start() {
    Properties properties = new Properties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServer);

    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, this.consumerGroup);

    consumer = new KafkaConsumer<>(properties);
  }

  @Override
  public void pollNext(SourcePipeline<Row> pipeline) throws Exception {
    for (KafkaSplit kafkaSplit : assignedKafkaSplits) {
      TopicPartition topicPartition = kafkaSplit.getTopicPartition();
      consumer.assign(Collections.singletonList(topicPartition));
      ConsumerRecords<byte[], byte[]> pullResult = consumer.poll(pollTimeout);
      List<ConsumerRecord<byte[], byte[]>> records = pullResult.records(topicPartition);

      if (Objects.isNull(records) || CollectionUtils.isEmpty(records)) {
        continue;
      }

      for (ConsumerRecord<byte[], byte[]> record : records) {
        Row deserialize = deserializationSchema.deserialize(record.value());
        pipeline.output(deserialize);
        if (kafkaSplit.getStartOffset() >= kafkaSplit.getEndOffset()) {
          LOG.info("Subtask {} kafka split {} in end of stream.",
              context.getIndexOfSubtask(),
              kafkaSplit);
          finishedKafkaSplits.add(kafkaSplit);
          break;
        }
      }
      kafkaSplit.setStartOffset(records.size() - 1);
      // if (!commitInCheckpoint) {
      //   consumer.seek(topicPartition, );
      // }
    }
    assignedKafkaSplits.removeAll(finishedKafkaSplits);
  }

  @Override
  public void addSplits(List<KafkaSplit> splits) {
    LOG.info("Subtask {} received {}(s) new splits, splits = {}.",
        context.getIndexOfSubtask(),
        CollectionUtils.size(splits),
        splits);

    assignedKafkaSplits.addAll(splits);
  }

  @Override
  public boolean hasMoreElements() {
    if (Boundedness.UNBOUNDEDNESS == boundedness) {
      return true;
    }
    if (noMoreSplits) {
      return CollectionUtils.size(assignedKafkaSplits) != 0;
    }
    return true;
  }

  @Override
  public List<KafkaSplit> snapshotState(long checkpointId) {
    LOG.info("Subtask {} start snapshotting for checkpoint id = {}.", context.getIndexOfSubtask(), checkpointId);
    if (commitInCheckpoint) {
      for (KafkaSplit kafkaSplit : assignedKafkaSplits) {
        consumer.seek(kafkaSplit.getTopicPartition(), kafkaSplit.getStartOffset());
        LOG.debug("Subtask {} committed topic partition = {} in checkpoint id = {}.",
            context.getIndexOfSubtask(),
            kafkaSplit.getTopicPartition(),
            checkpointId);
      }
    }
    return Lists.newArrayList(assignedKafkaSplits);
  }

  @Override
  public void close() throws Exception {
    if (Objects.nonNull(consumer)) {
      consumer.close();
      LOG.info("Subtask {} shutdown consumer.", context.getIndexOfSubtask());
    }
    LOG.info("Subtask {} closed.", context.getIndexOfSubtask());
  }

  @Override
  public void notifyNoMoreSplits() {
    LOG.info("Subtask {} received no more split signal.", context.getIndexOfSubtask());
    noMoreSplits = true;
  }
}
