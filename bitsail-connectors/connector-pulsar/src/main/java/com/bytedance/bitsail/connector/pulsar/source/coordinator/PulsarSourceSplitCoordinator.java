/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bytedance.bitsail.connector.pulsar.source.coordinator;

import com.bytedance.bitsail.base.connector.reader.v1.Boundedness;
import com.bytedance.bitsail.base.connector.reader.v1.SourceSplitCoordinator;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.connector.pulsar.common.config.v1.PulsarUtils;
import com.bytedance.bitsail.connector.pulsar.source.config.SourceConfiguration;
import com.bytedance.bitsail.connector.pulsar.source.enumerator.cursor.StartCursor;
import com.bytedance.bitsail.connector.pulsar.source.enumerator.subscriber.PulsarSubscriber;
import com.bytedance.bitsail.connector.pulsar.source.enumerator.topic.TopicPartition;
import com.bytedance.bitsail.connector.pulsar.source.enumerator.topic.TopicRange;
import com.bytedance.bitsail.connector.pulsar.source.enumerator.topic.range.RangeGenerator;
import com.bytedance.bitsail.connector.pulsar.source.split.v1.PulsarPartitionSplit;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.KeySharedPolicy;
import org.apache.pulsar.client.api.KeySharedPolicy.KeySharedPolicySticky;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Range;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.bytedance.bitsail.connector.pulsar.common.utils.PulsarExceptionUtils.sneakyClient;
import static com.bytedance.bitsail.connector.pulsar.source.config.CursorVerification.FAIL_ON_MISMATCH;
import static java.util.Collections.singletonList;

/** The enumerator class for pulsar source. */
@Internal
public class PulsarSourceSplitCoordinator
        implements SourceSplitCoordinator<PulsarPartitionSplit, PulsarSourceEnumStateV1> {

    private static final Logger LOG = LoggerFactory.getLogger(PulsarSourceSplitCoordinator.class);

    private PulsarAdmin pulsarAdmin;
    private PulsarClient pulsarClient;
    private final PulsarSubscriber subscriber;
    private final StartCursor startCursor;
    private final RangeGenerator rangeGenerator;
    private final BitSailConfiguration readerConfiguration;
    private final SourceConfiguration sourceConfiguration;
    private final Context<PulsarPartitionSplit, PulsarSourceEnumStateV1> context;
    private final SplitsAssignmentStateV1 assignmentState;


    public PulsarSourceSplitCoordinator(PulsarSubscriber subscriber, StartCursor startCursor, RangeGenerator rangeGenerator, BitSailConfiguration readerConfiguration,
                                        SourceConfiguration sourceConfiguration, Context<PulsarPartitionSplit, PulsarSourceEnumStateV1> context,
                                        SplitsAssignmentStateV1 assignmentState, Boundedness sourceBoundedness) {
        this.subscriber = subscriber;
        this.startCursor = startCursor;
        this.rangeGenerator = rangeGenerator;
        this.readerConfiguration = readerConfiguration;
        this.sourceConfiguration = sourceConfiguration;
        this.context = context;
        this.assignmentState = assignmentState;

    }

    @Override
    public void start() {
        this.pulsarAdmin = PulsarUtils.createAdmin(readerConfiguration);
        this.pulsarClient = PulsarUtils.createClient(readerConfiguration);
        rangeGenerator.open(readerConfiguration, sourceConfiguration);

        // Check the pulsar topic information and convert it into source split.
        if (sourceConfiguration.enablePartitionDiscovery()) {
            LOG.info(
                "Starting the PulsarSourceEnumerator for subscription {} "
                    + "with partition discovery interval of {} ms.",
                sourceConfiguration.getSubscriptionDesc(),
                sourceConfiguration.getPartitionDiscoveryIntervalMs());
            context.runAsync(
                this::getSubscribedTopicPartitions,
                this::checkPartitionChanges,
                0,
                sourceConfiguration.getPartitionDiscoveryIntervalMs());
        } else {
            LOG.info(
                "Starting the PulsarSourceEnumerator for subscription {} "
                    + "without periodic partition discovery.",
                sourceConfiguration.getSubscriptionDesc());
            context.runAsyncOnce(this::getSubscribedTopicPartitions, this::checkPartitionChanges);
        }
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        // the pulsar source pushes splits eagerly, rather than act upon split requests.
    }

    @Override
    public void addSplitsBack(List<PulsarPartitionSplit> splits, int subtaskId) {
        // Put the split back to current pending splits.
        assignmentState.putSplitsBackToPendingList(splits, subtaskId);

        // If the failed subtask has already restarted, we need to assign pending splits to it
        if (context.registeredReaders().contains(subtaskId)) {
            assignPendingPartitionSplits(singletonList(subtaskId));
        }
    }

    @Override
    public void addReader(int subtaskId) {
        LOG.debug(
            "Adding reader {} to PulsarSourceEnumerator for subscription {}.",
            subtaskId,
            sourceConfiguration.getSubscriptionDesc());
        assignPendingPartitionSplits(singletonList(subtaskId));
    }

    @Override
    public PulsarSourceEnumStateV1 snapshotState() {
        return assignmentState.snapshotState();
    }

    @Override
    public void close() {
        if (pulsarAdmin != null) {
            pulsarAdmin.close();
        }
    }

    // ----------------- private methods -------------------

    /**
     * List subscribed topic partitions on Pulsar cluster.
     *
     * <p>NOTE: This method should only be invoked in the worker executor thread, because it
     * requires network I/O with Pulsar cluster.
     *
     * @return Set of subscribed {@link TopicPartition}s
     */
    private Set<TopicPartition> getSubscribedTopicPartitions() {
        int parallelism = context.totalParallelism();
        Set<TopicPartition> partitions =
            subscriber.getSubscribedTopicPartitions(pulsarAdmin, rangeGenerator, parallelism);

        // Seek start position for given partitions.
        seekStartPosition(partitions);

        return partitions;
    }

    private void seekStartPosition(Set<TopicPartition> partitions) {
        ConsumerBuilder<byte[]> consumerBuilder = consumerBuilder();
        Set<String> seekedTopics = new HashSet<>();

        for (TopicPartition partition : partitions) {
            String topicName = partition.getFullTopicName();
            if (!assignmentState.containsTopic(topicName) && seekedTopics.add(topicName)) {
                try (Consumer<byte[]> consumer =
                         sneakyClient(() -> consumerBuilder.clone().topic(topicName).subscribe())) {
                    startCursor.seekPosition(
                        partition.getTopic(), partition.getPartitionId(), consumer);
                } catch (PulsarClientException e) {
                    if (sourceConfiguration.getVerifyInitialOffsets() == FAIL_ON_MISMATCH) {
                        throw new IllegalArgumentException(e);
                    } else {
                        // WARN_ON_MISMATCH would just print this warning message.
                        // No need to print the stacktrace.
                        LOG.warn(
                            "Failed to set initial consuming position for partition {}",
                            partition,
                            e);
                    }
                }
            }
        }
    }

    private ConsumerBuilder<byte[]> consumerBuilder() {
        ConsumerBuilder<byte[]> builder =
            PulsarUtils.createConsumerBuilder(pulsarClient, Schema.BYTES, readerConfiguration);
        if (sourceConfiguration.getSubscriptionType() == SubscriptionType.Key_Shared) {
            Range range = TopicRange.createFullRange().toPulsarRange();
            KeySharedPolicySticky keySharedPolicy = KeySharedPolicy.stickyHashRange().ranges(range);
            // Force this consume use sticky hash range in Key_Shared subscription.
            // Pulsar won't remove old message dispatcher before 2.8.2 release.
            builder.keySharedPolicy(keySharedPolicy);
        }

        return builder;
    }

    /**
     * Check if there's any partition changes within subscribed topic partitions fetched by worker
     * thread, and convert them to splits the assign them to pulsar readers.
     *
     * <p>NOTE: This method should only be invoked in the coordinator executor thread.
     *
     * @param fetchedPartitions Map from topic name to its description
     * @param throwable Exception in worker thread
     */
    private void checkPartitionChanges(Set<TopicPartition> fetchedPartitions, Throwable throwable) {
        if (throwable != null) {
            throw new FlinkRuntimeException(
                "Failed to list subscribed topic partitions due to ", throwable);
        }

        // Append the partitions into current assignment state.
        assignmentState.appendTopicPartitions(fetchedPartitions);
        List<Integer> registeredReaders = new ArrayList<>(context.registeredReaders());

        // Assign the new readers.
        assignPendingPartitionSplits(registeredReaders);
    }

    private void assignPendingPartitionSplits(List<Integer> pendingReaders) {
        // Validate the reader.
        pendingReaders.forEach(
            reader -> {
                if (!context.registeredReaders().contains(reader)) {
                    throw new IllegalStateException(
                        "Reader " + reader + " is not registered to source coordinator");
                }
            });

        // Assign splits to downstream readers.
        assignmentState.assignSplits(pendingReaders).ifPresent(assignment -> {
            assignment.forEach(context::assignSplit);
        });

        // If periodically partition discovery is disabled and the initializing discovery has done,
        // signal NoMoreSplitsEvent to pending readers
        if (assignmentState.noMoreNewPartitionSplits()) {
            LOG.debug(
                "No more PulsarPartitionSplits to assign."
                    + " Sending NoMoreSplitsEvent to reader {} in subscription {}.",
                pendingReaders,
                sourceConfiguration.getSubscriptionDesc());
            pendingReaders.forEach(this.context::signalNoMoreSplits);
        }
    }
}
