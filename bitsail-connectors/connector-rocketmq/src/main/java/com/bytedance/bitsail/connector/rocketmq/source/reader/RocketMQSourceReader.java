/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.bytedance.bitsail.connector.rocketmq.source.reader;

import com.bytedance.bitsail.base.connector.reader.v1.Boundedness;
import com.bytedance.bitsail.base.connector.reader.v1.SourcePipeline;
import com.bytedance.bitsail.base.connector.reader.v1.SourceReader;
import com.bytedance.bitsail.base.format.DeserializationSchema;
import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.connector.rocketmq.error.RocketMQErrorCode;
import com.bytedance.bitsail.connector.rocketmq.format.CountableDeserializationSchema;
import com.bytedance.bitsail.connector.rocketmq.format.RocketMQDeserializationSchema;
import com.bytedance.bitsail.connector.rocketmq.option.RocketMQSourceOptions;
import com.bytedance.bitsail.connector.rocketmq.source.split.RocketMQSplit;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.UUID;

public class RocketMQSourceReader implements SourceReader<Row, RocketMQSplit> {

  private static final Logger LOG = LoggerFactory.getLogger(RocketMQSourceReader.class);

  private static final String SOURCE_READER_INSTANCE_NAME_TEMPLATE = "rmq_source_reader:%s_%s_%s:%s";

  private final String cluster;
  private final String topic;
  private final String consumerGroup;
  private final String consumerTag;
  private final int pollBatchSize;
  private final long pollTimeout;
  private final Boundedness boundedness;
  private final boolean commitInCheckpoint;
  private final String accessKey;
  private final String secretKey;

  private BitSailConfiguration readerConfiguration;
  private transient Context context;
  private transient DefaultMQPullConsumer consumer;
  private transient Set<RocketMQSplit> assignedRocketMQSplits;
  private transient DeserializationSchema<byte[], Row> deserializationSchema;
  private transient boolean noMoreSplits;

  public RocketMQSourceReader(BitSailConfiguration readerConfiguration,
                              Context context,
                              Boundedness boundedness) {
    this.readerConfiguration = readerConfiguration;
    this.boundedness = boundedness;
    this.context = context;
    this.assignedRocketMQSplits = Sets.newHashSet();
    this.deserializationSchema = new CountableDeserializationSchema<byte[], Row>(readerConfiguration,
        new RocketMQDeserializationSchema(
            readerConfiguration,
            context.getTypeInfos())
    );
    this.noMoreSplits = false;

    cluster = readerConfiguration.get(RocketMQSourceOptions.CLUSTER);
    topic = readerConfiguration.get(RocketMQSourceOptions.TOPIC);
    consumerGroup = readerConfiguration.get(RocketMQSourceOptions.CONSUMER_GROUP);
    consumerTag = readerConfiguration.get(RocketMQSourceOptions.CONSUMER_TAG);
    pollBatchSize = readerConfiguration.get(RocketMQSourceOptions.POLL_BATCH_SIZE);
    pollTimeout = readerConfiguration.get(RocketMQSourceOptions.POLL_TIMEOUT);
    commitInCheckpoint = readerConfiguration.get(RocketMQSourceOptions.COMMIT_IN_CHECKPOINT);
    accessKey = readerConfiguration.get(RocketMQSourceOptions.ACCESS_KEY);
    secretKey = readerConfiguration.get(RocketMQSourceOptions.SECRET_KEY);
  }

  @Override
  public void start() {
    try {
      if (StringUtils.isNotEmpty(accessKey) && StringUtils.isNotEmpty(secretKey)) {
        AclClientRPCHook aclClientRPCHook = new AclClientRPCHook(
            new SessionCredentials(accessKey, secretKey));
        consumer = new DefaultMQPullConsumer(aclClientRPCHook);
      } else {
        consumer = new DefaultMQPullConsumer();
      }

      consumer.setConsumerGroup(consumerGroup);
      consumer.setNamesrvAddr(cluster);
      consumer.setInstanceName(String.format(SOURCE_READER_INSTANCE_NAME_TEMPLATE,
          cluster, topic, consumerGroup, UUID.randomUUID()));
      consumer.setConsumerPullTimeoutMillis(pollTimeout);
      consumer.start();
    } catch (Exception e) {
      throw BitSailException.asBitSailException(RocketMQErrorCode.CONSUMER_CREATE_FAILED, e);
    }
  }

  @Override
  public void pollNext(SourcePipeline<Row> pipeline) throws Exception {
    Set<RocketMQSplit> endedRocketMQSplits = Sets.newHashSet();
    for (RocketMQSplit rocketmqSplit : assignedRocketMQSplits) {
      MessageQueue messageQueue = rocketmqSplit.getMessageQueue();
      PullResult pullResult = consumer.pull(rocketmqSplit.getMessageQueue(),
          consumerTag,
          rocketmqSplit.getStartOffset(),
          pollBatchSize,
          pollTimeout);

      for (MessageExt message : pullResult.getMsgFoundList()) {
        Row deserialize = deserializationSchema.deserialize(message.getBody());
        pipeline.output(deserialize);
        rocketmqSplit.setStartOffset(message.getQueueOffset());
        if (deserializationSchema.isEndOfStream(deserialize)
            || rocketmqSplit.getStartOffset() >= rocketmqSplit.getEndOffset()) {
          LOG.info("Subtask {} rocketmq split {} in end of stream.",
              context.getIndexOfSubtask(),
              rocketmqSplit);
          endedRocketMQSplits.add(rocketmqSplit);
          continue;
        }
      }
      if (!commitInCheckpoint) {
        consumer.updateConsumeOffset(messageQueue, pullResult.getMaxOffset());
      }
    }
    assignedRocketMQSplits.removeAll(endedRocketMQSplits);
  }

  @Override
  public void addSplits(List<RocketMQSplit> splits) {
    LOG.info("Subtask {} received {}(s) new splits, splits = {}.",
        context.getIndexOfSubtask(),
        CollectionUtils.size(splits),
        splits);

    List<RocketMQSplit> dupeRocketMQSplits = Lists.newArrayList();
    for (RocketMQSplit split : splits) {
      if (assignedRocketMQSplits.contains(split)) {
        continue;
      }
      try {
        //consumer.assign(Collections.singleton(split.getMessageQueue()));
        //consumer.seek(split.getMessageQueue(), split.getStartOffset());
      } catch (Exception e) {
        LOG.error("Source reader {} seek offset {} for message queue {} failed.",
            context.getIndexOfSubtask(),
            split.getStartOffset(),
            split.getMessageQueue(),
            e);
        throw BitSailException.asBitSailException(RocketMQErrorCode.CONSUMER_SEEK_OFFSET_FAILED, e);
      }
      dupeRocketMQSplits.add(split);
      assignedRocketMQSplits.add(split);
    }
    //consumer.assign(dupeRocketMQSplits.stream()
    //    .map(RocketMQSplit::getMessageQueue)
    //    .collect(Collectors.toList()));
  }

  @Override
  public boolean hasMoreElements() {
    if (boundedness == Boundedness.UNBOUNDEDNESS) {
      return true;
    }
    if (noMoreSplits) {
      return CollectionUtils.size(assignedRocketMQSplits) != 0;
    }
    return true;
  }

  @Override
  public List<RocketMQSplit> snapshotState(long checkpointId) {
    LOG.info("Subtask {} start snapshotting.", context.getIndexOfSubtask());
    if (commitInCheckpoint) {
      for (RocketMQSplit rocketMQSplit : assignedRocketMQSplits) {
        try {
          consumer.updateConsumeOffset(rocketMQSplit.getMessageQueue(), rocketMQSplit.getStartOffset());
          LOG.debug("Subtask {} committed message queue = {}.", context.getIndexOfSubtask(),
              rocketMQSplit.getMessageQueue());
        } catch (MQClientException e) {
          throw new RuntimeException(e);
        }
      }
    }
    return Lists.newArrayList(assignedRocketMQSplits);
  }

  @Override
  public void close() throws Exception {
    if (consumer != null) {
      consumer.shutdown();
    }
  }

  @Override
  public void notifyNoMoreSplits() {
    noMoreSplits = true;
  }
}
