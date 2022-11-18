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

package com.bytedance.bitsail.connector.pulsar.source.reader.split.v1;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.connector.pulsar.common.config.v1.PulsarUtils;
import com.bytedance.bitsail.connector.pulsar.source.config.SourceConfiguration;
import com.bytedance.bitsail.connector.pulsar.source.enumerator.cursor.StopCursor;
import com.bytedance.bitsail.connector.pulsar.source.enumerator.topic.TopicPartition;
import com.bytedance.bitsail.connector.pulsar.source.reader.message.PulsarMessage;
import com.bytedance.bitsail.connector.pulsar.source.reader.message.PulsarMessageCollector;
import com.bytedance.bitsail.connector.pulsar.source.split.v1.PulsarPartitionSplit;

import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.util.Preconditions;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.KeySharedPolicy;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.bytedance.bitsail.connector.pulsar.common.utils.PulsarExceptionUtils.sneakyClient;
import static com.bytedance.bitsail.connector.pulsar.source.config.PulsarSourceConfigUtils.createConsumerBuilder;

public abstract class PulsarPartitionSplitReader {
    private static final Logger LOG = LoggerFactory.getLogger(PulsarPartitionSplitReader.class);

    protected final PulsarClient pulsarClient;
    protected final PulsarAdmin pulsarAdmin;
    protected final BitSailConfiguration configuration;
    protected final SourceConfiguration sourceConfiguration;
    protected final AtomicBoolean wakeup;

    protected Consumer<byte[]> pulsarConsumer;
    protected PulsarPartitionSplit registeredSplit;

    protected PulsarPartitionSplitReader(
        PulsarClient pulsarClient,
        PulsarAdmin pulsarAdmin,
        BitSailConfiguration readerConfiguration,
        SourceConfiguration sourceConfiguration) {
        this.pulsarClient = pulsarClient;
        this.pulsarAdmin = pulsarAdmin;
        this.configuration = readerConfiguration;
        this.sourceConfiguration = sourceConfiguration;
        this.wakeup = new AtomicBoolean(false);
    }

    public RecordsWithSplitIds<PulsarMessage<byte[]>> fetch() throws IOException {
        RecordsBySplits.Builder<PulsarMessage<byte[]>> builder = new RecordsBySplits.Builder<>();

        // Return when no split registered to this reader.
        if (pulsarConsumer == null || registeredSplit == null) {
            return builder.build();
        }

        // Set wakeup to false for start consuming.
        wakeup.compareAndSet(true, false);

        StopCursor stopCursor = registeredSplit.getStopCursor();
        String splitId = registeredSplit.splitId();
        PulsarMessageCollector collector = new PulsarMessageCollector<>(splitId, builder);
        Deadline deadline = Deadline.fromNow(sourceConfiguration.getMaxFetchTime());

        // Consume message from pulsar until it was woke up by flink reader.
        for (int messageNum = 0;
                messageNum < sourceConfiguration.getMaxFetchRecords()
                        && deadline.hasTimeLeft()
                        && isNotWakeup();
                messageNum++) {
            try {
                Duration timeout = deadline.timeLeftIfAny();
                Message<byte[]> message = pollMessage(timeout);
                if (message == null) {
                    break;
                }

                // Add message.
                collector.setMessage(message);
                collector.collect(message.getValue());

                // Acknowledge message if need.
                finishedPollMessage(message);

                if (stopCursor.shouldStop(message)) {
                    builder.addFinishedSplit(splitId);
                    break;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (TimeoutException e) {
                break;
            } catch (ExecutionException e) {
                LOG.error("Error in polling message from pulsar consumer.", e);
                break;
            } catch (Exception e) {
                throw new IOException(e);
            }
        }

        return builder.build();
    }

    public void handleSplitsChanges(SplitsChange<PulsarPartitionSplit> splitsChanges) {
        LOG.debug("Handle split changes {}", splitsChanges);

        // Get all the partition assignments and stopping offsets.
        if (!(splitsChanges instanceof SplitsAddition)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "The SplitChange type of %s is not supported.",
                            splitsChanges.getClass()));
        }

        if (registeredSplit != null) {
            throw new IllegalStateException("This split reader have assigned split.");
        }

        List<PulsarPartitionSplit> newSplits = splitsChanges.splits();
        Preconditions.checkArgument(
                newSplits.size() == 1, "This pulsar split reader only support one split.");
        PulsarPartitionSplit newSplit = newSplits.get(0);

        // Create pulsar consumer.
        Consumer<byte[]> consumer = createPulsarConsumer(newSplit);

        // Open start & stop cursor.
        newSplit.open(pulsarAdmin);

        // Start Consumer.
        startConsumer(newSplit, consumer);

        LOG.info("Register split {} consumer for current reader.", newSplit);
        this.registeredSplit = newSplit;
        this.pulsarConsumer = consumer;
    }

    public void wakeUp() {
        wakeup.compareAndSet(false, true);
    }

    public void close() {
        if (pulsarConsumer != null) {
            sneakyClient(() -> pulsarConsumer.close());
        }
    }

    @Nullable
    protected abstract Message<byte[]> pollMessage(Duration timeout)
            throws ExecutionException, InterruptedException, PulsarClientException;

    protected abstract void finishedPollMessage(Message<byte[]> message);

    protected abstract void startConsumer(PulsarPartitionSplit split, Consumer<byte[]> consumer);

    // --------------------------- Helper Methods -----------------------------

    protected boolean isNotWakeup() {
        return !wakeup.get();
    }

    /** Create a specified {@link Consumer} by the given split information. */
    protected Consumer<byte[]> createPulsarConsumer(PulsarPartitionSplit split) {
        return createPulsarConsumer(split.getPartition());
    }

    /** Create a specified {@link Consumer} by the given topic partition. */
    protected Consumer<byte[]> createPulsarConsumer(TopicPartition partition) {
        ConsumerBuilder<byte[]> consumerBuilder =
                PulsarUtils.createConsumerBuilder(pulsarClient, Schema.BYTES, configuration);

        consumerBuilder.topic(partition.getFullTopicName());

        // Add KeySharedPolicy for Key_Shared subscription.
        if (sourceConfiguration.getSubscriptionType() == SubscriptionType.Key_Shared) {
            KeySharedPolicy policy =
                    KeySharedPolicy.stickyHashRange().ranges(partition.getPulsarRange());
            consumerBuilder.keySharedPolicy(policy);
        }

        // Create the consumer configuration by using common utils.
        return sneakyClient(consumerBuilder::subscribe);
    }


    public void commit(TopicPartition partition, MessageId offsetsToCommit) {

    };
}
