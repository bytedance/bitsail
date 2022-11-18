package com.bytedance.bitsail.connector.pulsar.common.config.v1;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.option.ConfigOption;
import com.bytedance.bitsail.connector.pulsar.source.config.CursorVerification;
import com.bytedance.bitsail.connector.pulsar.source.enumerator.topic.TopicMetadata;
import com.bytedance.bitsail.connector.pulsar.source.enumerator.topic.TopicNameUtils;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.ConsumerCryptoFailureAction;
import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.ProxyProtocol;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.RegexSubscriptionMode;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.auth.AuthenticationDisabled;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.bytedance.bitsail.connector.pulsar.common.config.v1.PulsarOptionsV1.*;
import static com.bytedance.bitsail.connector.pulsar.common.utils.PulsarExceptionUtils.sneakyClient;
import static com.bytedance.bitsail.connector.pulsar.source.PulsarSourceOptionsV1.*;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.function.Function.identity;
import static org.apache.pulsar.client.api.SizeUnit.BYTES;

public class PulsarUtils {

  public static final String TOPIC_MODE_LIST = "list";
  public static final String TOPIC_MODE_PATTERN = "pattern";
  public static final String TOPIC_LIST_DELIMITER = ",";

  /**
   * PulsarAdmin shares almost the same configuration with PulsarClient, but we separate this
   * create method for directly create it.
   */
  public static PulsarAdmin createAdmin(BitSailConfiguration configuration) {
    PulsarAdminBuilder builder = PulsarAdmin.builder();

    setOptionValue(configuration, PULSAR_ADMIN_URL, builder::serviceHttpUrl);
    builder.authentication(createAuthentication(configuration));
    setOptionValue(
	configuration, PULSAR_TLS_TRUST_CERTS_FILE_PATH, builder::tlsTrustCertsFilePath);
    setOptionValue(
	configuration,
	PULSAR_TLS_ALLOW_INSECURE_CONNECTION,
	builder::allowTlsInsecureConnection);
    setOptionValue(
	configuration,
	PULSAR_TLS_HOSTNAME_VERIFICATION_ENABLE,
	builder::enableTlsHostnameVerification);
    setOptionValue(configuration, PULSAR_USE_KEY_STORE_TLS, builder::useKeyStoreTls);
    setOptionValue(configuration, PULSAR_SSL_PROVIDER, builder::sslProvider);
    setOptionValue(configuration, PULSAR_TLS_TRUST_STORE_TYPE, builder::tlsTrustStoreType);
    setOptionValue(configuration, PULSAR_TLS_TRUST_STORE_PATH, builder::tlsTrustStorePath);
    setOptionValue(
	configuration, PULSAR_TLS_TRUST_STORE_PASSWORD, builder::tlsTrustStorePassword);
    setOptionValue(configuration, PULSAR_TLS_CIPHERS, TreeSet::new, builder::tlsCiphers);
    setOptionValue(configuration, PULSAR_TLS_PROTOCOLS, TreeSet::new, builder::tlsProtocols);
    setOptionValue(
	configuration,
	PULSAR_CONNECT_TIMEOUT,
	v -> builder.connectionTimeout(v, MILLISECONDS));
    setOptionValue(
	configuration, PULSAR_READ_TIMEOUT, v -> builder.readTimeout(v, MILLISECONDS));
    setOptionValue(
	configuration,
	PULSAR_REQUEST_TIMEOUT,
	v -> builder.requestTimeout(v, MILLISECONDS));
    setOptionValue(
	configuration,
	PULSAR_AUTO_CERT_REFRESH_TIME,
	v -> builder.autoCertRefreshTime(v, MILLISECONDS));

    return sneakyClient(builder::build);
  }

  public static PulsarClient createClient(BitSailConfiguration configuration) {
    ClientBuilder builder = PulsarClient.builder();

    setOptionValue(configuration, PULSAR_SERVICE_URL, builder::serviceUrl);
    setOptionValue(configuration, PULSAR_LISTENER_NAME, builder::listenerName);
    builder.authentication(createAuthentication(configuration));
    setOptionValue(
	configuration,
	PULSAR_OPERATION_TIMEOUT_MS,
	timeout -> builder.operationTimeout(timeout, MILLISECONDS));
    setOptionValue(configuration, PULSAR_NUM_IO_THREADS, builder::ioThreads);
    setOptionValue(configuration, PULSAR_NUM_LISTENER_THREADS, builder::listenerThreads);
    setOptionValue(configuration, PULSAR_CONNECTIONS_PER_BROKER, builder::connectionsPerBroker);
    setOptionValue(configuration, PULSAR_USE_TCP_NO_DELAY, builder::enableTcpNoDelay);
    setOptionValue(
	configuration, PULSAR_TLS_TRUST_CERTS_FILE_PATH, builder::tlsTrustCertsFilePath);
    setOptionValue(
	configuration,
	PULSAR_TLS_ALLOW_INSECURE_CONNECTION,
	builder::allowTlsInsecureConnection);
    setOptionValue(
	configuration,
	PULSAR_TLS_HOSTNAME_VERIFICATION_ENABLE,
	builder::enableTlsHostnameVerification);
    setOptionValue(configuration, PULSAR_USE_KEY_STORE_TLS, builder::useKeyStoreTls);
    setOptionValue(configuration, PULSAR_SSL_PROVIDER, builder::sslProvider);
    setOptionValue(configuration, PULSAR_TLS_TRUST_STORE_TYPE, builder::tlsTrustStoreType);
    setOptionValue(configuration, PULSAR_TLS_TRUST_STORE_PATH, builder::tlsTrustStorePath);
    setOptionValue(
	configuration, PULSAR_TLS_TRUST_STORE_PASSWORD, builder::tlsTrustStorePassword);
    setOptionValue(configuration, PULSAR_TLS_CIPHERS, TreeSet::new, builder::tlsCiphers);
    setOptionValue(configuration, PULSAR_TLS_PROTOCOLS, TreeSet::new, builder::tlsProtocols);
    setOptionValue(
	configuration,
	PULSAR_MEMORY_LIMIT_BYTES,
	bytes -> builder.memoryLimit(bytes, BYTES));
    setOptionValue(
	configuration,
	PULSAR_STATS_INTERVAL_SECONDS,
	v -> builder.statsInterval(v, SECONDS));
    setOptionValue(
	configuration,
	PULSAR_CONCURRENT_LOOKUP_REQUEST,
	builder::maxConcurrentLookupRequests);
    setOptionValue(configuration, PULSAR_MAX_LOOKUP_REQUEST, builder::maxLookupRequests);
    setOptionValue(configuration, PULSAR_MAX_LOOKUP_REDIRECTS, builder::maxLookupRedirects);
    setOptionValue(
	configuration,
	PULSAR_MAX_NUMBER_OF_REJECTED_REQUEST_PER_CONNECTION,
	builder::maxNumberOfRejectedRequestPerConnection);
    setOptionValue(
	configuration,
	PULSAR_KEEP_ALIVE_INTERVAL_SECONDS,
	v -> builder.keepAliveInterval(v, SECONDS));
    setOptionValue(
	configuration,
	PULSAR_CONNECTION_TIMEOUT_MS,
	v -> builder.connectionTimeout(v, MILLISECONDS));
    setOptionValue(
	configuration,
	PULSAR_INITIAL_BACKOFF_INTERVAL_NANOS,
	v -> builder.startingBackoffInterval(v, NANOSECONDS));
    setOptionValue(
	configuration,
	PULSAR_MAX_BACKOFF_INTERVAL_NANOS,
	v -> builder.maxBackoffInterval(v, NANOSECONDS));
    setOptionValue(configuration, PULSAR_ENABLE_BUSY_WAIT, builder::enableBusyWait);
    if (configuration.fieldExists(PULSAR_PROXY_SERVICE_URL.key())) {
      String proxyServiceUrl = configuration.getString(PULSAR_PROXY_SERVICE_URL.key());
      ProxyProtocol proxyProtocol = getProxyProtocol(configuration.getString(PULSAR_PROXY_PROTOCOL.key()));
      builder.proxyServiceUrl(proxyServiceUrl, proxyProtocol);
    }
    setOptionValue(configuration, PULSAR_ENABLE_TRANSACTION, builder::enableTransaction);

    return sneakyClient(builder::build);
  }

  /** Create a pulsar consumer builder by using the given Configuration. */
  public static <T> ConsumerBuilder<T> createConsumerBuilder(
      PulsarClient client, Schema<T> schema, BitSailConfiguration configuration) {
    ConsumerBuilder<T> builder = client.newConsumer(schema);

    setOptionValue(configuration, PULSAR_SUBSCRIPTION_NAME, builder::subscriptionName);
    setOptionValue(
	configuration, PULSAR_ACK_TIMEOUT_MILLIS, v -> builder.ackTimeout(v, MILLISECONDS));
    setOptionValue(configuration, PULSAR_ACK_RECEIPT_ENABLED, builder::isAckReceiptEnabled);
    setOptionValue(
	configuration,
	PULSAR_TICK_DURATION_MILLIS,
	v -> builder.ackTimeoutTickTime(v, MILLISECONDS));
    setOptionValue(
	configuration,
	PULSAR_NEGATIVE_ACK_REDELIVERY_DELAY_MICROS,
	v -> builder.negativeAckRedeliveryDelay(v, MICROSECONDS));
    setOptionValue(configuration, PULSAR_SUBSCRIPTION_TYPE, e -> builder.subscriptionType(PulsarUtils.getSubscriptionType(e)));
    setOptionValue(configuration, PULSAR_SUBSCRIPTION_MODE, e -> builder.subscriptionMode(PulsarUtils.getSubscriptionMode(e)));
    setOptionValue(configuration, PULSAR_CRYPTO_FAILURE_ACTION, e -> builder.cryptoFailureAction(PulsarUtils.getCryptoFailureAction(e)));
    setOptionValue(configuration, PULSAR_RECEIVER_QUEUE_SIZE, builder::receiverQueueSize);
    setOptionValue(
	configuration,
	PULSAR_ACKNOWLEDGEMENTS_GROUP_TIME_MICROS,
	v -> builder.acknowledgmentGroupTime(v, MICROSECONDS));
    setOptionValue(
	configuration,
	PULSAR_REPLICATE_SUBSCRIPTION_STATE,
	builder::replicateSubscriptionState);
    setOptionValue(
	configuration,
	PULSAR_MAX_TOTAL_RECEIVER_QUEUE_SIZE_ACROSS_PARTITIONS,
	builder::maxTotalReceiverQueueSizeAcrossPartitions);
    setOptionValue(configuration, PULSAR_CONSUMER_NAME, builder::consumerName);
    setOptionValue(configuration, PULSAR_READ_COMPACTED, builder::readCompacted);
    setOptionValue(configuration, PULSAR_PRIORITY_LEVEL, builder::priorityLevel);
    setOptionValue(configuration, PULSAR_CONSUMER_PROPERTIES, builder::properties);
    setOptionValue(
	configuration,
	PULSAR_SUBSCRIPTION_INITIAL_POSITION,
	e -> builder.subscriptionInitialPosition(PulsarUtils.getSubscriptionInitialPosition(e)));
    createDeadLetterPolicy(configuration).ifPresent(builder::deadLetterPolicy);
    setOptionValue(
	configuration,
	PULSAR_AUTO_UPDATE_PARTITIONS_INTERVAL_SECONDS,
	v -> builder.autoUpdatePartitionsInterval(v, SECONDS));
    setOptionValue(configuration, PULSAR_RETRY_ENABLE, builder::enableRetry);
    setOptionValue(
	configuration,
	PULSAR_MAX_PENDING_CHUNKED_MESSAGE,
	builder::maxPendingChunkedMessage);
    setOptionValue(
	configuration,
	PULSAR_AUTO_ACK_OLDEST_CHUNKED_MESSAGE_ON_QUEUE_FULL,
	builder::autoAckOldestChunkedMessageOnQueueFull);
    setOptionValue(
	configuration,
	PULSAR_EXPIRE_TIME_OF_INCOMPLETE_CHUNKED_MESSAGE_MILLIS,
	v -> builder.expireTimeOfIncompleteChunkedMessage(v, MILLISECONDS));
    setOptionValue(configuration, PULSAR_POOL_MESSAGES, builder::poolMessages);

    return builder;
  }

  private static SubscriptionInitialPosition getSubscriptionInitialPosition(String value) {
    SubscriptionInitialPosition type = SubscriptionInitialPosition.Latest;
    for (SubscriptionInitialPosition s : SubscriptionInitialPosition.values()) {
      if (s.name().equalsIgnoreCase(value)) {
	type = s;
      }
    }
    return type;
  }

  private static ConsumerCryptoFailureAction getCryptoFailureAction(String value) {
    ConsumerCryptoFailureAction type = ConsumerCryptoFailureAction.FAIL;
    for (ConsumerCryptoFailureAction s : ConsumerCryptoFailureAction.values()) {
      if (s.name().equalsIgnoreCase(value)) {
	type = s;
      }
    }
    return type;
  }

  private static Optional<DeadLetterPolicy> createDeadLetterPolicy(BitSailConfiguration configuration) {
    if (configuration.fieldExists(PULSAR_MAX_REDELIVER_COUNT.key())
	|| configuration.fieldExists(PULSAR_RETRY_LETTER_TOPIC.key())
	|| configuration.fieldExists(PULSAR_DEAD_LETTER_TOPIC.key())) {
      DeadLetterPolicy.DeadLetterPolicyBuilder builder = DeadLetterPolicy.builder();

      setOptionValue(configuration, PULSAR_MAX_REDELIVER_COUNT, builder::maxRedeliverCount);
      setOptionValue(configuration, PULSAR_RETRY_LETTER_TOPIC, builder::retryLetterTopic);
      setOptionValue(configuration, PULSAR_DEAD_LETTER_TOPIC, builder::deadLetterTopic);

      return Optional.of(builder.build());
    } else {
      return Optional.empty();
    }
  }

  private static ProxyProtocol getProxyProtocol(String value) {
    ProxyProtocol res = ProxyProtocol.SNI;
    for (ProxyProtocol test : ProxyProtocol.values()) {
      if (test.name().equalsIgnoreCase(value)) {
        res = test;
      }
    }
    return res;
  }

  /**
   * Create the {@link Authentication} instance for both {@code PulsarClient} and {@code
   * PulsarAdmin}. If the user didn't provide configuration, a {@link AuthenticationDisabled}
   * instance would be returned.
   *
   * <p>This method behavior is the same as the pulsar command line tools.
   */
  private static Authentication createAuthentication(BitSailConfiguration configuration) {
    if (configuration.fieldExists(PULSAR_AUTH_PLUGIN_CLASS_NAME.key())) {
      String authPluginClassName = configuration.getString(PULSAR_AUTH_PLUGIN_CLASS_NAME.key());

      if (configuration.fieldExists(PULSAR_AUTH_PARAMS.key())) {
	String authParamsString = configuration.getString(PULSAR_AUTH_PARAMS.key());
	return sneakyClient(
	    () -> AuthenticationFactory.create(authPluginClassName, authParamsString));
      } else if (configuration.fieldExists(PULSAR_AUTH_PARAM_MAP.key())) {
	Map<String, String> paramsMap = (Map<String, String>) configuration.getMap(PULSAR_AUTH_PARAM_MAP.key());
	return sneakyClient(
	    () -> AuthenticationFactory.create(authPluginClassName, paramsMap));
      } else {
	throw new IllegalArgumentException(
	    String.format(
		"No %s or %s provided",
		PULSAR_AUTH_PARAMS.key(), PULSAR_AUTH_PARAM_MAP.key()));
      }
    }

    return AuthenticationDisabled.INSTANCE;
  }

  /** Get the option value str from given config, convert it into the real value instance. */
  public static <F, T> T getOptionValue(
      BitSailConfiguration configuration, ConfigOption<F> option, Function<F, T> convertor) {
    F value = configuration.get(option);
    if (value != null) {
      return convertor.apply(value);
    } else {
      return null;
    }
  }

  /** Set the config option's value to a given builder. */
  public static <T> void setOptionValue(
      BitSailConfiguration configuration, ConfigOption<T> option, Consumer<T> setter) {
    setOptionValue(configuration, option, identity(), setter);
  }

  /**
   * Query the config option's value, convert it into a required type, set it to a given builder.
   */
  public static <T, V> void setOptionValue(
      BitSailConfiguration configuration,
      ConfigOption<T> option,
      Function<T, V> convertor,
      Consumer<V> setter) {
    if (configuration.fieldExists(option.key())) {
      V value = getOptionValue(configuration, option, convertor);
      setter.accept(value);
    }
  }


  public static boolean matchesRegexSubscriptionMode(String topic, RegexSubscriptionMode subscriptionMode) {
    TopicName topicName = TopicName.get(topic);
    // Filter the topic persistence.
    switch (subscriptionMode) {
      case PersistentOnly:
	return topicName.isPersistent();
      case NonPersistentOnly:
	return !topicName.isPersistent();
      default:
	// RegexSubscriptionMode.AllTopics
	return true;
    }
  }

  public static TopicMetadata queryTopicMetadata(PulsarAdmin pulsarAdmin, String topicName) {
    // Drop the complete topic name for a clean partitioned topic name.
    String completeTopicName = TopicNameUtils.topicName(topicName);
    try {
      PartitionedTopicMetadata metadata =
	  pulsarAdmin.topics().getPartitionedTopicMetadata(completeTopicName);
      return new TopicMetadata(topicName, metadata.partitions);
    } catch (PulsarAdminException e) {
      if (e.getStatusCode() == 404) {
	// Return null for skipping the topic metadata query.
	return null;
      } else {
	// This method would cause the failure for subscriber.
	throw new IllegalStateException(e);
      }
    }
  }

  public static SubscriptionMode getSubscriptionMode(String value) {
    SubscriptionMode subscriptionMode = SubscriptionMode.Durable;
    for (SubscriptionMode mode : SubscriptionMode.values()) {
      if (mode.name().equalsIgnoreCase(value)) {
	subscriptionMode = mode;
      }
    }
    return subscriptionMode;
  }
    public static RegexSubscriptionMode getRegexSubscriptionMode(String value) {
    RegexSubscriptionMode subscriptionMode = RegexSubscriptionMode.AllTopics;
    for (RegexSubscriptionMode mode : RegexSubscriptionMode.values()) {
      if (mode.name().equalsIgnoreCase(value)) {
	subscriptionMode = mode;
      }
    }
    return subscriptionMode;
  }

  public static List<String> getTopicList(String value) {
    List<String> topicList = new LinkedList<>();
    for (String t: value.split(TOPIC_LIST_DELIMITER)) {
      topicList.add(t.trim());
    }
    return topicList;
  }

  public static SubscriptionType getSubscriptionType(String value) {
    SubscriptionType subscriptionType = SubscriptionType.Exclusive;
    for (SubscriptionType s : SubscriptionType.values()) {
      if (s.name().equalsIgnoreCase(value)) {
	subscriptionType = s;
      }
    }
    return subscriptionType;
  }

  public static CursorVerification getCursorVerification(String value) {
    CursorVerification type = CursorVerification.WARN_ON_MISMATCH;
    for (CursorVerification s : CursorVerification.values()) {
      if (s.name().equalsIgnoreCase(value)) {
	type = s;
      }
    }
    return type;
  }

}
