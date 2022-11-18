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

import com.bytedance.bitsail.common.option.ConfigOption;
import com.bytedance.bitsail.common.option.ConfigOptions;
import com.bytedance.bitsail.connector.pulsar.source.config.CursorVerification;

import org.apache.pulsar.client.api.ConsumerCryptoFailureAction;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyMap;

@SuppressWarnings("java:S1192")
public final class PulsarSourceOptionsV1 {

    public static final String READER_PREFIX = "job.reader.";
    // Pulsar source connector config prefix.
    public static final String SOURCE_CONFIG_PREFIX = READER_PREFIX + "pulsar.source.";
    // Pulsar consumer API config prefix.
    public static final String CONSUMER_CONFIG_PREFIX = READER_PREFIX + "pulsar.consumer.";

    private PulsarSourceOptionsV1() {
        // This is a constant class
    }

    ///////////////////////////////////////////////////////////////////////////////
    //
    // The configuration for pulsar source part.
    // All the configuration listed below should have the pulsar.source prefix.
    //
    ///////////////////////////////////////////////////////////////////////////////

    public static final ConfigOption<Long> PULSAR_PARTITION_DISCOVERY_INTERVAL_MS =
            ConfigOptions.key(SOURCE_CONFIG_PREFIX + "partitionDiscoveryIntervalMs")
                                        .defaultValue(Duration.ofSeconds(30).toMillis());

    public static final ConfigOption<Boolean> PULSAR_ENABLE_AUTO_ACKNOWLEDGE_MESSAGE =
            ConfigOptions.key(SOURCE_CONFIG_PREFIX + "enableAutoAcknowledgeMessage")
                    .defaultValue(false);

    public static final ConfigOption<Long> PULSAR_AUTO_COMMIT_CURSOR_INTERVAL =
            ConfigOptions.key(SOURCE_CONFIG_PREFIX + "autoCommitCursorInterval")
                                        .defaultValue(Duration.ofSeconds(5).toMillis());

    public static final ConfigOption<Long> PULSAR_TRANSACTION_TIMEOUT_MILLIS =
            ConfigOptions.key(SOURCE_CONFIG_PREFIX + "transactionTimeoutMillis")
                                        .defaultValue(Duration.ofHours(3).toMillis());

    public static final ConfigOption<Long> PULSAR_MAX_FETCH_TIME =
            ConfigOptions.key(SOURCE_CONFIG_PREFIX + "maxFetchTime")
                                        .defaultValue(Duration.ofSeconds(100).toMillis());

    public static final ConfigOption<Integer> PULSAR_MAX_FETCH_RECORDS =
            ConfigOptions.key(SOURCE_CONFIG_PREFIX + "maxFetchRecords")
                                        .defaultValue(100);

    public static final ConfigOption<String> PULSAR_VERIFY_INITIAL_OFFSETS =
            ConfigOptions.key(SOURCE_CONFIG_PREFIX + "verifyInitialOffsets")
                    .defaultValue(CursorVerification.WARN_ON_MISMATCH.name());

    ///////////////////////////////////////////////////////////////////////////////
    //
    // The configuration for ConsumerConfigurationData part.
    // All the configuration listed below should have the pulsar.consumer prefix.
    //
    ///////////////////////////////////////////////////////////////////////////////

    public static final ConfigOption<String> PULSAR_SUBSCRIPTION_NAME =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "subscriptionName")
                                        .noDefaultValue(String.class);

    public static final ConfigOption<String> PULSAR_SUBSCRIPTION_TYPE =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "subscriptionType")
                    .defaultValue(SubscriptionType.Shared.name());

    public static final ConfigOption<String> PULSAR_SUBSCRIPTION_MODE =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "subscriptionMode")
                    .defaultValue(SubscriptionMode.Durable.name());

    public static final ConfigOption<Integer> PULSAR_RECEIVER_QUEUE_SIZE =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "receiverQueueSize")
                                        .defaultValue(1000);

    public static final ConfigOption<Long> PULSAR_ACKNOWLEDGEMENTS_GROUP_TIME_MICROS =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "acknowledgementsGroupTimeMicros")
                                        .defaultValue(TimeUnit.MILLISECONDS.toMicros(100));

    public static final ConfigOption<Long> PULSAR_NEGATIVE_ACK_REDELIVERY_DELAY_MICROS =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "negativeAckRedeliveryDelayMicros")
                                        .defaultValue(TimeUnit.MINUTES.toMicros(1));

    public static final ConfigOption<Integer>
            PULSAR_MAX_TOTAL_RECEIVER_QUEUE_SIZE_ACROSS_PARTITIONS =
                    ConfigOptions.key(
                                    CONSUMER_CONFIG_PREFIX
                                            + "maxTotalReceiverQueueSizeAcrossPartitions")
                                                        .defaultValue(50000);

    public static final ConfigOption<String> PULSAR_CONSUMER_NAME =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "consumerName")
                                        .noDefaultValue(String.class);

    public static final ConfigOption<Long> PULSAR_ACK_TIMEOUT_MILLIS =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "ackTimeoutMillis")
                                        .defaultValue(0L);

    public static final ConfigOption<Long> PULSAR_TICK_DURATION_MILLIS =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "tickDurationMillis")
                                        .defaultValue(1000L);

    public static final ConfigOption<Integer> PULSAR_PRIORITY_LEVEL =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "priorityLevel")
                                        .defaultValue(0);

    public static final ConfigOption<Integer> PULSAR_MAX_PENDING_CHUNKED_MESSAGE =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "maxPendingChunkedMessage")
                                        .defaultValue(10);

    public static final ConfigOption<Boolean> PULSAR_AUTO_ACK_OLDEST_CHUNKED_MESSAGE_ON_QUEUE_FULL =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "autoAckOldestChunkedMessageOnQueueFull")
                                        .defaultValue(false);

    public static final ConfigOption<Long> PULSAR_EXPIRE_TIME_OF_INCOMPLETE_CHUNKED_MESSAGE_MILLIS =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "expireTimeOfIncompleteChunkedMessageMillis")
                                        .defaultValue(60 * 1000L);

    public static final ConfigOption<String> PULSAR_CRYPTO_FAILURE_ACTION =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "cryptoFailureAction")
                    .defaultValue(ConsumerCryptoFailureAction.FAIL.name());

    // TODO (Dian): adapt to json format
    public static final ConfigOption<Map<String, String>> PULSAR_CONSUMER_PROPERTIES =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "properties")
                                        .defaultValue(emptyMap());

    public static final ConfigOption<Boolean> PULSAR_READ_COMPACTED =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "readCompacted")
                                        .defaultValue(false);

    public static final ConfigOption<String>
            PULSAR_SUBSCRIPTION_INITIAL_POSITION =
                    ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "subscriptionInitialPosition")
                            .defaultValue(SubscriptionInitialPosition.Latest.name());

    // The config set for DeadLetterPolicy

    /**
     * Dead letter policy for consumers.
     *
     * <p>By default, some messages are probably redelivered many times, even to the extent that it
     * never stops.
     *
     * <p>By using the dead letter mechanism, messages have the max redelivery count. When exceeding
     * the maximum number of redeliveries, messages are sent to the Dead Letter Topic and
     * acknowledged automatically.
     *
     * <p>You can enable the dead letter mechanism by setting deadLetterPolicy.
     *
     * <p>Example <code>pulsar.consumer.deadLetterPolicy.maxRedeliverCount = 10</code> Default dead
     * letter topic name is <strong>{TopicName}-{Subscription}-DLQ</strong>.
     *
     * <p>To set a custom dead letter topic name:
     *
     * <pre><code>
     * pulsar.consumer.deadLetterPolicy.maxRedeliverCount = 10
     * pulsar.consumer.deadLetterPolicy.deadLetterTopic = your-topic-name
     * </code></pre>
     *
     * <p>When specifying the dead letter policy while not specifying ackTimeoutMillis, you can set
     * the ack timeout to 30000 millisecond.
     */
    public static final ConfigOption<Integer> PULSAR_MAX_REDELIVER_COUNT =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "deadLetterPolicy.maxRedeliverCount")
                                        .defaultValue(0);

    public static final ConfigOption<String> PULSAR_RETRY_LETTER_TOPIC =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "deadLetterPolicy.retryLetterTopic")
                                        .noDefaultValue(String.class);
    public static final ConfigOption<String> PULSAR_DEAD_LETTER_TOPIC =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "deadLetterPolicy.deadLetterTopic")
                                        .noDefaultValue(String.class);

    public static final ConfigOption<Boolean> PULSAR_RETRY_ENABLE =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "retryEnable")
                                        .defaultValue(false);

    public static final ConfigOption<Integer> PULSAR_AUTO_UPDATE_PARTITIONS_INTERVAL_SECONDS =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "autoUpdatePartitionsIntervalSeconds")
                                        .defaultValue(60);

    public static final ConfigOption<Boolean> PULSAR_REPLICATE_SUBSCRIPTION_STATE =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "replicateSubscriptionState")
                                        .defaultValue(false);

    public static final ConfigOption<Boolean> PULSAR_ACK_RECEIPT_ENABLED =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "ackReceiptEnabled")
                                        .defaultValue(false);

    public static final ConfigOption<Boolean> PULSAR_POOL_MESSAGES =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "poolMessages")
                                        .defaultValue(false);

    public static final String PULSAR_START_CURSOR_MODE_LATEST = "latest";
    public static final String PULSAR_START_CURSOR_MODE_EARLIEST = "earliest";
    public static final String PULSAR_START_CURSOR_MODE_TIMESTAMP = "timestamp";
    public static final ConfigOption<String> PULSAR_START_CURSOR_MODE =
        ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "startCursorMode")
                        .defaultValue(PULSAR_START_CURSOR_MODE_LATEST);
    public static final ConfigOption<Long> PULSAR_START_CURSOR_TIMESTAMP =
        ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "startCursorTimestamp")
                        .noDefaultValue(Long.class);
    public static final String PULSAR_STOP_CURSOR_MODE_LATEST = "latest";
    public static final String PULSAR_STOP_CURSOR_MODE_NEVER = "never";
    public static final String PULSAR_STOP_CURSOR_MODE_TIMESTAMP = "timestamp";
    public static final ConfigOption<String> PULSAR_STOP_CURSOR_MODE =
        ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "stopCursorMode")
                        .defaultValue(PULSAR_STOP_CURSOR_MODE_NEVER);
    public static final ConfigOption<Long> PULSAR_STOP_CURSOR_TIMESTAMP =
        ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "stopCursorTimestamp")
                        .noDefaultValue(Long.class);
    
}
