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

package com.bytedance.bitsail.connector.kafka.source.coordinator;

import com.bytedance.bitsail.base.connector.reader.v1.Boundedness;
import com.bytedance.bitsail.base.connector.reader.v1.SourceSplitCoordinator;
import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.exception.CommonErrorCode;
import com.bytedance.bitsail.connector.base.source.split.SplitAssigner;
import com.bytedance.bitsail.connector.kafka.constants.KafkaConstants;
import com.bytedance.bitsail.connector.kafka.error.KafkaErrorCode;
import com.bytedance.bitsail.connector.kafka.option.KafkaSourceOptions;
import com.bytedance.bitsail.connector.kafka.source.split.KafkaSplit;
import com.bytedance.bitsail.connector.kafka.source.split.KafkaState;
import com.bytedance.bitsail.connector.kafka.util.KafkaUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static com.bytedance.bitsail.connector.kafka.constants.KafkaConstants.CONSUMER_OFFSET_TIMESTAMP_KEY;


public class KafkaSourceSplitCoordinator implements SourceSplitCoordinator<KafkaSplit, KafkaState> {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaSourceSplitCoordinator.class);

  private final SourceSplitCoordinator.Context<KafkaSplit, KafkaState> context;
  private final BitSailConfiguration jobConfiguration;
  private final Boundedness boundedness;

  private final Set<TopicPartition> discoveredPartitions;
  private final Map<TopicPartition, String> assignedPartitions;
  private final Map<Integer, Set<KafkaSplit>> pendingKafkaSplitAssignment;
  private final long discoveryInternal;
  private final Properties properties = new Properties();

  private String bootstrapServers;
  private String topic;
  private String consumerGroup;
  private String startupMode;
  private long consumerOffsetTimestamp;
  private Map<TopicPartition, Long> consumerStopOffset;

  private transient SplitAssigner<TopicPartition> splitAssigner;
  private transient AdminClient adminClient;

  public KafkaSourceSplitCoordinator(
      SourceSplitCoordinator.Context<KafkaSplit, KafkaState> context,
      BitSailConfiguration jobConfiguration,
      Boundedness boundedness) {
    this.context = context;
    this.jobConfiguration = jobConfiguration;
    this.boundedness = boundedness;
    this.discoveryInternal = jobConfiguration.get(KafkaSourceOptions.DISCOVERY_INTERNAL);
    this.properties.putAll(jobConfiguration.get(KafkaSourceOptions.PROPERTIES));
    this.pendingKafkaSplitAssignment = Maps.newConcurrentMap();
    this.consumerOffsetTimestamp = jobConfiguration.get(KafkaSourceOptions.STARTUP_MODE_TIMESTAMP);

    this.discoveredPartitions = new HashSet<>();
    if (context.isRestored()) {
      KafkaState restoreState = context.getRestoreState();
      assignedPartitions = restoreState.getAssignedWithSplitsIds();
      discoveredPartitions.addAll(assignedPartitions.keySet());
    } else {
      assignedPartitions = Maps.newHashMap();
    }

    prepareConsumerProperties();
  }

  @Override
  public void addReader(int subtaskId) {
    LOG.info(
        "Adding reader {} to Kafka Split Coordinator for consumer group {}.",
        subtaskId,
        consumerGroup);
    notifyReaderAssignmentResult();
  }

  @Override
  public void addSplitsBack(List<KafkaSplit> splits, int subtaskId) {
    LOG.info("Source reader {} return splits {}.", subtaskId, splits);
    addSplitChangeToPendingAssignment(new HashSet<>(splits));
    notifyReaderAssignmentResult();
  }

  private Map<TopicPartition, Long> listOffsets(Collection<TopicPartition> partitions, OffsetSpec offsetSpec)
      throws ExecutionException, InterruptedException {
    Map<TopicPartition, OffsetSpec> topicPartitionOffsets =
        partitions.stream()
            .collect(
                Collectors.toMap(
                    partition -> partition, __ -> offsetSpec));

    return adminClient
        .listOffsets(topicPartitionOffsets)
        .all()
        .thenApply(
            result -> {
              Map<TopicPartition, Long> offsets = new HashMap<>();
              result.forEach(
                  (tp, offsetsResultInfo) -> {
                    if (offsetsResultInfo != null) {
                      offsets.put(tp, offsetsResultInfo.offset());
                    }
                  });
              return offsets;
            })
        .get();
  }

  public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions)
      throws ExecutionException, InterruptedException {
    return listOffsets(partitions, OffsetSpec.latest());
  }

  public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions)
      throws ExecutionException, InterruptedException {
    return listOffsets(partitions, OffsetSpec.earliest());
  }

  @Override
  public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
    // empty
  }

  @Override
  public KafkaState snapshotState() throws Exception {
    return new KafkaState(assignedPartitions);
  }

  @Override
  public void close() {
    if (adminClient != null) {
      adminClient.close();
    }
  }

  // TODO: add more check
  private void prepareConsumerProperties() {
    bootstrapServers = jobConfiguration.get(KafkaSourceOptions.BOOTSTRAP_SERVERS);
    topic = jobConfiguration.get(KafkaSourceOptions.TOPIC);
    consumerGroup = jobConfiguration.get(KafkaSourceOptions.CONSUMER_GROUP);
    startupMode = jobConfiguration.get(KafkaSourceOptions.STARTUP_MODE);
    if (StringUtils.equalsIgnoreCase(startupMode, CONSUMER_OFFSET_TIMESTAMP_KEY)) {
      consumerOffsetTimestamp = jobConfiguration.get(KafkaSourceOptions.STARTUP_MODE_TIMESTAMP);
    }
  }

  private void prepareKafkaAdminClient() {
    try {
      adminClient = KafkaUtils.prepareKafkaAdminClient(jobConfiguration, properties);
    } catch (Exception e) {
      throw BitSailException.asBitSailException(KafkaErrorCode.CONSUMER_CREATE_FAILED, e);
    }
  }

  @Override
  public void start() {
    prepareKafkaAdminClient();
    splitAssigner = new FairKafkaSplitAssigner(jobConfiguration, assignedPartitions);
    if (discoveryInternal > 0) {
      context.runAsync(
          this::fetchTopicPartitions,
          this::handleTopicPartitionChanged,
          0,
          discoveryInternal
      );
    } else {
      context.runAsyncOnce(
          this::fetchTopicPartitions,
          this::handleTopicPartitionChanged
      );
    }
  }

  private Set<KafkaSplit> fetchTopicPartitions() throws ExecutionException, InterruptedException {
    Collection<TopicPartition> fetchedTopicPartitions = Sets.newHashSet(getSubscribedTopicPartitions(adminClient));
    discoveredPartitions.addAll(fetchedTopicPartitions);

    Map<TopicPartition, Long> beginningOffsets = getStartOffset(fetchedTopicPartitions);
    Map<TopicPartition, Long> endOffsets = endOffsets(fetchedTopicPartitions);

    Set<KafkaSplit> pendingAssignedPartitions = Sets.newHashSet();
    for (TopicPartition topicPartition : fetchedTopicPartitions) {
      if (assignedPartitions.containsKey(topicPartition)) {
        continue;
      }

      pendingAssignedPartitions.add(
          KafkaSplit.builder()
              .topicPartition(topicPartition)
              .startOffset(beginningOffsets.get(topicPartition))
              .endOffset(endOffsets.get(topicPartition))
              .splitId(splitAssigner.assignSplitId(topicPartition))
              .build()
      );
    }
    return pendingAssignedPartitions;
  }

  private Map<TopicPartition, Long> getStartOffset(Collection<TopicPartition> fetchedTopicPartitions)
      throws ExecutionException, InterruptedException {
    Map<TopicPartition, Long> topicPartitionOffsets = null;
    switch (startupMode) {
      case KafkaConstants.CONSUMER_OFFSET_EARLIEST_KEY:
        topicPartitionOffsets = beginningOffsets(fetchedTopicPartitions);
        break;
      case KafkaConstants.CONSUMER_OFFSET_LATEST_KEY:
        topicPartitionOffsets = endOffsets(fetchedTopicPartitions);
        break;
      case KafkaConstants.CONSUMER_OFFSET_TIMESTAMP_KEY:
        topicPartitionOffsets = listOffsets(fetchedTopicPartitions, OffsetSpec.forTimestamp(consumerOffsetTimestamp));
        break;
      default:
        throw BitSailException.asBitSailException(
            KafkaErrorCode.CONSUMER_FETCH_OFFSET_FAILED,
            String.format("Consumer startup mode = %s not support right now.", startupMode));
    }
    return topicPartitionOffsets;
  }

  private Map<String, TopicDescription> getTopicMetadata(
      AdminClient adminClient, Set<String> topicNames) {
    try {
      return adminClient.describeTopics(topicNames).allTopicNames().get();
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to get metadata for topics %s.", topicNames), e);
    }
  }

  private Set<TopicPartition> getSubscribedTopicPartitions(AdminClient adminClient) {
    LOG.debug("Fetching descriptions for topics: {}", this.topic);
    final Map<String, TopicDescription> topicMetadata =
        getTopicMetadata(adminClient, new HashSet<>(Collections.singletonList(topic)));

    Set<TopicPartition> subscribedPartitions = new HashSet<>();
    for (TopicDescription topic : topicMetadata.values()) {
      for (TopicPartitionInfo partition : topic.partitions()) {
        subscribedPartitions.add(new TopicPartition(topic.name(), partition.partition()));
      }
    }
    return subscribedPartitions;
  }

  private void handleTopicPartitionChanged(Set<KafkaSplit> pendingAssignedSplits,
                                           Throwable throwable) {
    if (throwable != null) {
      throw BitSailException.asBitSailException(
          CommonErrorCode.INTERNAL_ERROR,
          String.format("Failed to fetch kafka offset for the topic: %s", topic), throwable);
    }

    if (CollectionUtils.isEmpty(pendingAssignedSplits)) {
      return;
    }
    addSplitChangeToPendingAssignment(pendingAssignedSplits);
    notifyReaderAssignmentResult();
  }

  private void notifyReaderAssignmentResult() {
    Map<Integer, List<KafkaSplit>> tmpKafkaSplitAssignments = new HashMap<>();

    for (Integer pendingAssignmentReader : pendingKafkaSplitAssignment.keySet()) {
      if (CollectionUtils.isNotEmpty(pendingKafkaSplitAssignment.get(pendingAssignmentReader))
          && context.registeredReaders().contains(pendingAssignmentReader)) {
        tmpKafkaSplitAssignments.put(pendingAssignmentReader, Lists.newArrayList(pendingKafkaSplitAssignment.get(pendingAssignmentReader)));
      }
    }

    for (Integer pendingAssignmentReader : tmpKafkaSplitAssignments.keySet()) {
      LOG.info("Assigning splits to reader {}, splits = {}.", pendingAssignmentReader,
          tmpKafkaSplitAssignments.get(pendingAssignmentReader));

      context.assignSplit(pendingAssignmentReader,
          tmpKafkaSplitAssignments.get(pendingAssignmentReader));
      Set<KafkaSplit> removes = pendingKafkaSplitAssignment.remove(pendingAssignmentReader);
      removes.forEach(removeSplit -> {
        assignedPartitions.put(removeSplit.getTopicPartition(), removeSplit.getSplitId());
      });
      LOG.info("Assigned splits to reader {}", pendingAssignmentReader);

      if (Boundedness.BOUNDEDNESS == boundedness) {
        LOG.info("Signal reader {} no more splits assigned in future.", pendingAssignmentReader);
        context.signalNoMoreSplits(pendingAssignmentReader);
      }
    }
  }

  private synchronized void addSplitChangeToPendingAssignment(Set<KafkaSplit> newKafkaSplits) {
    int numReader = context.totalParallelism();
    for (KafkaSplit split : newKafkaSplits) {
      int readerIndex = splitAssigner.assignToReader(split.getSplitId(), numReader);
      pendingKafkaSplitAssignment.computeIfAbsent(readerIndex, r -> new HashSet<>())
          .add(split);
    }
    LOG.debug("Kafka splits {} finished assignment.", newKafkaSplits);
  }
}
