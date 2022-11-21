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

package com.bytedance.bitsail.connector.pulsar.source;

import com.bytedance.bitsail.base.connector.reader.v1.Boundedness;
import com.bytedance.bitsail.base.connector.reader.v1.Source;
import com.bytedance.bitsail.base.connector.reader.v1.SourceReader;
import com.bytedance.bitsail.base.connector.reader.v1.SourceSplitCoordinator;
import com.bytedance.bitsail.base.execution.ExecutionEnviron;
import com.bytedance.bitsail.base.execution.Mode;
import com.bytedance.bitsail.base.extension.ParallelismComputable;
import com.bytedance.bitsail.base.parallelism.ParallelismAdvice;
import com.bytedance.bitsail.base.serializer.BinarySerializer;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.option.CommonOptions;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.connector.pulsar.common.config.v1.PulsarOptionsV1;
import com.bytedance.bitsail.connector.pulsar.common.config.v1.PulsarUtils;
import com.bytedance.bitsail.connector.pulsar.source.config.SourceConfiguration;
import com.bytedance.bitsail.connector.pulsar.source.coordinator.PulsarSourceEnumStateV1;
import com.bytedance.bitsail.connector.pulsar.source.coordinator.PulsarSourceSplitCoordinator;
import com.bytedance.bitsail.connector.pulsar.source.coordinator.SplitsAssignmentStateV1;
import com.bytedance.bitsail.connector.pulsar.source.enumerator.cursor.StartCursor;
import com.bytedance.bitsail.connector.pulsar.source.enumerator.cursor.StopCursor;
import com.bytedance.bitsail.connector.pulsar.source.enumerator.subscriber.PulsarSubscriber;
import com.bytedance.bitsail.connector.pulsar.source.enumerator.topic.TopicMetadata;
import com.bytedance.bitsail.connector.pulsar.source.enumerator.topic.range.FullRangeGenerator;
import com.bytedance.bitsail.connector.pulsar.source.enumerator.topic.range.RangeGenerator;
import com.bytedance.bitsail.connector.pulsar.source.enumerator.topic.range.UniformRangeGenerator;
import com.bytedance.bitsail.connector.pulsar.source.reader.source.v1.PulsarOrderedSourceReaderV1;
import com.bytedance.bitsail.connector.pulsar.source.reader.source.v1.PulsarUnorderedSourceReaderV1;
import com.bytedance.bitsail.connector.pulsar.source.reader.split.v1.PulsarOrderedPartitionSplitReader;
import com.bytedance.bitsail.connector.pulsar.source.reader.split.v1.PulsarUnorderedPartitionSplitReader;
import com.bytedance.bitsail.connector.pulsar.source.split.v1.PulsarPartitionSplit;
import com.bytedance.bitsail.connector.pulsar.source.split.v1.PulsarPartitionSplitSerializer;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.RegexSubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClient;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;

import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import static com.bytedance.bitsail.connector.pulsar.source.PulsarSourceOptionsV1.PULSAR_START_CURSOR_MODE_EARLIEST;
import static com.bytedance.bitsail.connector.pulsar.source.PulsarSourceOptionsV1.PULSAR_START_CURSOR_MODE_LATEST;
import static com.bytedance.bitsail.connector.pulsar.source.PulsarSourceOptionsV1.PULSAR_START_CURSOR_MODE_TIMESTAMP;
import static com.bytedance.bitsail.connector.pulsar.source.PulsarSourceOptionsV1.PULSAR_STOP_CURSOR_MODE_LATEST;
import static com.bytedance.bitsail.connector.pulsar.source.PulsarSourceOptionsV1.PULSAR_STOP_CURSOR_MODE_NEVER;
import static com.bytedance.bitsail.connector.pulsar.source.PulsarSourceOptionsV1.PULSAR_STOP_CURSOR_MODE_TIMESTAMP;
import static com.bytedance.bitsail.connector.pulsar.source.PulsarSourceOptionsV1.PULSAR_SUBSCRIPTION_TYPE;

@PublicEvolving
@Slf4j
public final class PulsarSourceV1
        implements Source<Row, PulsarPartitionSplit, PulsarSourceEnumStateV1>, ParallelismComputable {
    private static final long serialVersionUID = 7773108631275567433L;
    private BitSailConfiguration readerConfiguration;

    private BitSailConfiguration commonConfiguration;
    private static final int DEFAULT_PULSAR_PARALLELISM_THRESHOLD = 4;
    private RangeGenerator rangeGenerator;
    private PulsarSubscriber subscriber;
    private StartCursor startCursor;
    private SourceConfiguration sourceConfiguration;
    private StopCursor stopCursor;
    private SplitsAssignmentStateV1 assignmentState;

    @Override
    public void configure(ExecutionEnviron execution, BitSailConfiguration readerConfiguration) {
        this.readerConfiguration = readerConfiguration;
        this.commonConfiguration = execution.getCommonConfiguration();
        SubscriptionType subscriptionType =
            PulsarUtils.getSubscriptionType(readerConfiguration.getString(PULSAR_SUBSCRIPTION_TYPE.key()));
        if (subscriptionType == SubscriptionType.Key_Shared) {
            if (rangeGenerator == null) {
                log.warn(
                    "No range generator provided for key_shared subscription,"
                        + " we would use the DivideRangeGenerator as the default range generator.");
                this.rangeGenerator = new UniformRangeGenerator();
            }
        } else {
            // Override the range generator.
            this.rangeGenerator = new FullRangeGenerator();
        }
        String topics = readerConfiguration.getString(PulsarOptionsV1.PULSAR_TOPICS.key());
        String topicMode = readerConfiguration.getString(PulsarOptionsV1.PULSAR_TOPIC_MODE.key());
        String topicSubscriptionMode = readerConfiguration.getString(PulsarSourceOptionsV1.PULSAR_SUBSCRIPTION_MODE.key());
        if (PulsarUtils.TOPIC_MODE_PATTERN.equalsIgnoreCase(topicMode)) {
            this.subscriber =
                PulsarSubscriber.getTopicPatternSubscriber(Pattern.compile(topics), PulsarUtils.getRegexSubscriptionMode(topicSubscriptionMode));

        } else {
            this.subscriber = PulsarSubscriber.getTopicListSubscriber(PulsarUtils.getTopicList(topics));
        }
        setStartCursor(readerConfiguration);
        setStopCursor(readerConfiguration);

        sourceConfiguration = new SourceConfiguration(readerConfiguration);

        assignmentState =
            new SplitsAssignmentStateV1(stopCursor, sourceConfiguration);
    }

    private void setStartCursor(BitSailConfiguration readerConfiguration) {
        String startCursorMode = readerConfiguration.getString(PulsarSourceOptionsV1.PULSAR_START_CURSOR_MODE.key(), PULSAR_START_CURSOR_MODE_LATEST);
        switch (startCursorMode) {
            case PULSAR_START_CURSOR_MODE_LATEST:
                this.startCursor = StartCursor.latest();
                break;
            case PULSAR_START_CURSOR_MODE_EARLIEST:
                this.startCursor = StartCursor.earliest();
                break;
            case PULSAR_START_CURSOR_MODE_TIMESTAMP:
                Long cursorTimestamp = readerConfiguration.getLong(PulsarSourceOptionsV1.PULSAR_START_CURSOR_TIMESTAMP.key());
                if (cursorTimestamp == null) {
                    cursorTimestamp = System.currentTimeMillis();
                }
                this.startCursor = StartCursor.fromMessageTime(cursorTimestamp);
                break;
            default:
                this.startCursor = StartCursor.latest();
                break;
        }
    }

    private void setStopCursor(BitSailConfiguration readerConfiguration) {
        String stopCursorMode = readerConfiguration.get(PulsarSourceOptionsV1.PULSAR_STOP_CURSOR_MODE);
        switch (stopCursorMode) {
            case PULSAR_STOP_CURSOR_MODE_LATEST:
                this.stopCursor = StopCursor.latest();
                break;
            case PULSAR_STOP_CURSOR_MODE_NEVER:
                this.stopCursor = StopCursor.never();
                break;
            case PULSAR_STOP_CURSOR_MODE_TIMESTAMP:
                Long cursorTimestamp = readerConfiguration.getLong(PulsarSourceOptionsV1.PULSAR_STOP_CURSOR_TIMESTAMP.key());
                if (cursorTimestamp == null) {
                    cursorTimestamp = System.currentTimeMillis();
                }
                this.stopCursor = StopCursor.atEventTime(cursorTimestamp);
                break;
            default:
                this.stopCursor = StopCursor.never();
                break;
        }
    }

    @Override
    public Boundedness getSourceBoundedness() {
        return Mode.BATCH.equals(Mode.getJobRunMode(commonConfiguration.get(CommonOptions.JOB_TYPE))) ?
            Boundedness.BOUNDEDNESS :
            Boundedness.UNBOUNDEDNESS;
    }


    @Override
    public SourceSplitCoordinator<PulsarPartitionSplit, PulsarSourceEnumStateV1> createSplitCoordinator(
        SourceSplitCoordinator.Context<PulsarPartitionSplit, PulsarSourceEnumStateV1> coordinatorContext) {
        return new PulsarSourceSplitCoordinator(
            subscriber,
            startCursor,
            rangeGenerator,
            readerConfiguration,
            sourceConfiguration,
            coordinatorContext,
            assignmentState,
            getSourceBoundedness());
    }

    @Override
    public String getReaderName() {
        return "pulsar";
    }

    @Override
    public ParallelismAdvice getParallelismAdvice(BitSailConfiguration commonConfiguration,
                                                  BitSailConfiguration pulsarConfiguration,
                                                  ParallelismAdvice upstreamAdvice) throws Exception {
        String topics = pulsarConfiguration.getString(PulsarOptionsV1.PULSAR_TOPICS.key());
        String topicMode = pulsarConfiguration.getString(PulsarOptionsV1.PULSAR_TOPIC_MODE.key());
        String topicSubscriptionMode = pulsarConfiguration.getString(PulsarSourceOptionsV1.PULSAR_SUBSCRIPTION_MODE.key());
        PulsarAdmin pulsarAdmin = PulsarUtils.createAdmin(pulsarConfiguration);
        Integer partitions;
        if (PulsarUtils.TOPIC_MODE_PATTERN.equalsIgnoreCase(topicMode)) {
            RegexSubscriptionMode subscriptionMode = PulsarUtils.getRegexSubscriptionMode(topicSubscriptionMode);
            TopicName destination = TopicName.get(topics);
            NamespaceName namespaceName = destination.getNamespaceObject();
            String namespace = namespaceName.toString();
            Pattern topicPattern = Pattern.compile(topics);
            partitions = pulsarAdmin
                .namespaces()
                .getTopics(namespace)
                .parallelStream()
                .filter(t -> PulsarUtils.matchesRegexSubscriptionMode(t, subscriptionMode))
                .filter(topic -> topicPattern.matcher(topic).find())
                .map(topic -> PulsarUtils.queryTopicMetadata(pulsarAdmin, topic))
                .filter(Objects::nonNull)
                .map(TopicMetadata::getPartitionSize)
                .reduce(0, Integer::sum);
        } else {
            List<String> topicList = PulsarUtils.getTopicList(topics);
            partitions = topicList.parallelStream()
                .map(topic -> PulsarUtils.queryTopicMetadata(pulsarAdmin, topic))
                .filter(Objects::nonNull)
                .map(TopicMetadata::getPartitionSize)
                .reduce(0, Integer::sum);
        }


        try {
            int adviceParallelism = Math.max(partitions / DEFAULT_PULSAR_PARALLELISM_THRESHOLD, 1);

            return ParallelismAdvice.builder()
                .adviceParallelism(adviceParallelism)
                .enforceDownStreamChain(true)
                .build();
        } finally {
        }
    }


    @Override
    public SourceReader<Row, PulsarPartitionSplit> createReader(SourceReader.Context readerContext) {
        PulsarClient pulsarClient = PulsarUtils.createClient(readerConfiguration);
        PulsarAdmin pulsarAdmin = PulsarUtils.createAdmin(readerConfiguration);

        // Create different pulsar source reader by subscription type.
        SubscriptionType subscriptionType = sourceConfiguration.getSubscriptionType();
        if (subscriptionType == SubscriptionType.Failover
            || subscriptionType == SubscriptionType.Exclusive) {
            // Create a ordered split reader supplier.
            Supplier<PulsarOrderedPartitionSplitReader> splitReaderSupplier =
                () ->
                    new PulsarOrderedPartitionSplitReader(
                        pulsarClient,
                        pulsarAdmin,
                        readerConfiguration,
                        sourceConfiguration);

            return new PulsarOrderedSourceReaderV1(
                new FutureCompletingBlockingQueue<>(2),
                splitReaderSupplier,
                readerConfiguration,
                readerContext,
                sourceConfiguration,
                pulsarClient,
                pulsarAdmin);
        } else if (subscriptionType == SubscriptionType.Shared
            || subscriptionType == SubscriptionType.Key_Shared) {
            TransactionCoordinatorClient coordinatorClient =
                ((PulsarClientImpl) pulsarClient).getTcClient();
            if (coordinatorClient == null
                && !sourceConfiguration.isEnableAutoAcknowledgeMessage()) {
                throw new IllegalStateException("Transaction is required but didn't enabled");
            }

            Supplier<PulsarUnorderedPartitionSplitReader> splitReaderSupplier =
                () ->
                    new PulsarUnorderedPartitionSplitReader(
                        pulsarClient,
                        pulsarAdmin,
                        readerConfiguration,
                        sourceConfiguration,
                        coordinatorClient);

            return new PulsarUnorderedSourceReaderV1(
                new FutureCompletingBlockingQueue<>(2),
                splitReaderSupplier,
                readerConfiguration,
                readerContext,
                sourceConfiguration,
                pulsarClient,
                pulsarAdmin,
                coordinatorClient);
        } else {
            throw new UnsupportedOperationException(
                "This subscription type is not " + subscriptionType + " supported currently.");
        }
    }


    @Override
    public BinarySerializer<PulsarPartitionSplit> getSplitSerializer() {
        return PulsarPartitionSplitSerializer.INSTANCE;
    }
}
