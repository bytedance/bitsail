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

package com.bytedance.bitsail.connector.pulsar.common.config.v1;

import com.bytedance.bitsail.common.option.ConfigOption;
import com.bytedance.bitsail.common.option.ConfigOptions;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.docs.ConfigGroup;
import org.apache.flink.annotation.docs.ConfigGroups;
import org.apache.flink.configuration.description.Description;
import org.apache.pulsar.client.api.ProxyProtocol;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.bytedance.bitsail.connector.pulsar.common.config.PulsarOptions.ADMIN_CONFIG_PREFIX;
import static com.bytedance.bitsail.connector.pulsar.common.config.PulsarOptions.CLIENT_CONFIG_PREFIX;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.flink.configuration.description.TextElement.code;
import static org.apache.flink.configuration.description.TextElement.text;

/**
 * Configuration for Pulsar Client, these config options would be used for both source, sink and
 * table.
 */
@PublicEvolving
@ConfigGroups(
        groups = {
            @ConfigGroup(name = "PulsarClient", keyPrefix = CLIENT_CONFIG_PREFIX),
            @ConfigGroup(name = "PulsarAdmin", keyPrefix = ADMIN_CONFIG_PREFIX)
        })
@SuppressWarnings("java:S1192")
public final class PulsarOptionsV1 {
    public static final String READER_PREFIX = "job.reader.";


    // Pulsar client API config prefix.
    public static final String CLIENT_CONFIG_PREFIX = READER_PREFIX + "pulsar.client.";
    // Pulsar admin API config prefix.
    public static final String ADMIN_CONFIG_PREFIX = READER_PREFIX + "pulsar.admin.";

    private PulsarOptionsV1() {
        // This is a constant class
    }

    ///////////////////////////////////////////////////////////////////////////////
    //
    // The configuration for ClientConfigurationData part.
    // All the configuration listed below should have the pulsar.client prefix.
    //
    ///////////////////////////////////////////////////////////////////////////////

    public static final ConfigOption<String> PULSAR_SERVICE_URL =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "serviceUrl")
                                        .noDefaultValue(String.class);

    public static final ConfigOption<String> PULSAR_AUTH_PLUGIN_CLASS_NAME =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "authPluginClassName")
                                        .noDefaultValue(String.class);

    public static final ConfigOption<String> PULSAR_AUTH_PARAMS =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "authParams")
                                        .noDefaultValue(String.class);

    public static final ConfigOption<Map> PULSAR_AUTH_PARAM_MAP =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "authParamMap")
                                        .noDefaultValue(Map.class);

    public static final ConfigOption<Integer> PULSAR_OPERATION_TIMEOUT_MS =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "operationTimeoutMs")
                                        .defaultValue(30000);

    public static final ConfigOption<Long> PULSAR_STATS_INTERVAL_SECONDS =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "statsIntervalSeconds")
                                        .defaultValue(60L);

    public static final ConfigOption<Integer> PULSAR_NUM_IO_THREADS =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "numIoThreads")
                                        .defaultValue(1);

    public static final ConfigOption<Integer> PULSAR_NUM_LISTENER_THREADS =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "numListenerThreads")
                                        .defaultValue(1);

    public static final ConfigOption<Integer> PULSAR_CONNECTIONS_PER_BROKER =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "connectionsPerBroker")
                                        .defaultValue(1);

    public static final ConfigOption<Boolean> PULSAR_USE_TCP_NO_DELAY =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "useTcpNoDelay")
                                        .defaultValue(true);

    public static final ConfigOption<String> PULSAR_TLS_TRUST_CERTS_FILE_PATH =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "tlsTrustCertsFilePath")
                                        .defaultValue("");

    public static final ConfigOption<Boolean> PULSAR_TLS_ALLOW_INSECURE_CONNECTION =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "tlsAllowInsecureConnection")
                                        .defaultValue(false);

    public static final ConfigOption<Boolean> PULSAR_TLS_HOSTNAME_VERIFICATION_ENABLE =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "tlsHostnameVerificationEnable")
                                        .defaultValue(false);

    public static final ConfigOption<Integer> PULSAR_CONCURRENT_LOOKUP_REQUEST =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "concurrentLookupRequest")
                                        .defaultValue(5000);

    public static final ConfigOption<Integer> PULSAR_MAX_LOOKUP_REQUEST =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "maxLookupRequest")
                                        .defaultValue(50000);

    public static final ConfigOption<Integer> PULSAR_MAX_LOOKUP_REDIRECTS =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "maxLookupRedirects")
                                        .defaultValue(20);

    public static final ConfigOption<Integer> PULSAR_MAX_NUMBER_OF_REJECTED_REQUEST_PER_CONNECTION =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "maxNumberOfRejectedRequestPerConnection")
                                        .defaultValue(50);

    public static final ConfigOption<Integer> PULSAR_KEEP_ALIVE_INTERVAL_SECONDS =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "keepAliveIntervalSeconds")
                                        .defaultValue(30);

    public static final ConfigOption<Integer> PULSAR_CONNECTION_TIMEOUT_MS =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "connectionTimeoutMs")
                                        .defaultValue(10000);

    // TODO This option would be exposed by Pulsar's ClientBuilder in the next Pulsar release.
    public static final ConfigOption<Integer> PULSAR_REQUEST_TIMEOUT_MS =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "requestTimeoutMs")
                                        .defaultValue(60000);

    public static final ConfigOption<Long> PULSAR_INITIAL_BACKOFF_INTERVAL_NANOS =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "initialBackoffIntervalNanos")
                                        .defaultValue(TimeUnit.MILLISECONDS.toNanos(100));

    public static final ConfigOption<Long> PULSAR_MAX_BACKOFF_INTERVAL_NANOS =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "maxBackoffIntervalNanos")
                                        .defaultValue(SECONDS.toNanos(60));

    public static final ConfigOption<Boolean> PULSAR_ENABLE_BUSY_WAIT =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "enableBusyWait")
                                        .defaultValue(false);

    public static final ConfigOption<String> PULSAR_LISTENER_NAME =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "listenerName")
                                        .noDefaultValue(String.class);

    public static final ConfigOption<Boolean> PULSAR_USE_KEY_STORE_TLS =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "useKeyStoreTls")
                                        .defaultValue(false);

    public static final ConfigOption<String> PULSAR_SSL_PROVIDER =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "sslProvider")
                                        .noDefaultValue(String.class);

    public static final ConfigOption<String> PULSAR_TLS_TRUST_STORE_TYPE =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "tlsTrustStoreType")
                                        .defaultValue("JKS");

    public static final ConfigOption<String> PULSAR_TLS_TRUST_STORE_PATH =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "tlsTrustStorePath")
                                        .noDefaultValue(String.class);

    public static final ConfigOption<String> PULSAR_TLS_TRUST_STORE_PASSWORD =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "tlsTrustStorePassword")
                                        .noDefaultValue(String.class);

    // The real config type is Set<String>, you should provided a json str here.
    public static final ConfigOption<List<String>> PULSAR_TLS_CIPHERS =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "tlsCiphers")
                    .defaultValue(new ArrayList<>());

    public static final ConfigOption<List<String>> PULSAR_TLS_PROTOCOLS =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "tlsProtocols")
                .defaultValue(new ArrayList<>());

    public static final ConfigOption<Long> PULSAR_MEMORY_LIMIT_BYTES =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "memoryLimitBytes")
                                        .defaultValue(0L);

    public static final ConfigOption<String> PULSAR_PROXY_SERVICE_URL =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "proxyServiceUrl")
                                        .noDefaultValue(String.class);

    public static final ConfigOption<ProxyProtocol> PULSAR_PROXY_PROTOCOL =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "proxyProtocol")
                    .defaultValue(ProxyProtocol.SNI);

    public static final ConfigOption<Boolean> PULSAR_ENABLE_TRANSACTION =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "enableTransaction")
                                        .defaultValue(false);

    ///////////////////////////////////////////////////////////////////////////////
    //
    // The configuration for PulsarAdmin part.
    // All the configuration listed below should have the pulsar.admin prefix.
    //
    ///////////////////////////////////////////////////////////////////////////////

    public static final ConfigOption<String> PULSAR_ADMIN_URL =
            ConfigOptions.key(ADMIN_CONFIG_PREFIX + "adminUrl")
                                        .noDefaultValue(String.class);
    public static final ConfigOption<String> PULSAR_TOPIC_MODE =
        ConfigOptions.key(ADMIN_CONFIG_PREFIX + "topicMode")
                        .noDefaultValue(String.class);
    public static final ConfigOption<String> PULSAR_TOPICS =
        ConfigOptions.key(ADMIN_CONFIG_PREFIX + "topics")
                        .noDefaultValue(String.class);

    public static final ConfigOption<Integer> PULSAR_CONNECT_TIMEOUT =
            ConfigOptions.key(ADMIN_CONFIG_PREFIX + "connectTimeout")
                                        .defaultValue(60000);

    public static final ConfigOption<Integer> PULSAR_READ_TIMEOUT =
            ConfigOptions.key(ADMIN_CONFIG_PREFIX + "readTimeout")
                                        .defaultValue(60000);

    public static final ConfigOption<Integer> PULSAR_REQUEST_TIMEOUT =
            ConfigOptions.key(ADMIN_CONFIG_PREFIX + "requestTimeout")
                                        .defaultValue(300000);

    public static final ConfigOption<Integer> PULSAR_AUTO_CERT_REFRESH_TIME =
            ConfigOptions.key(ADMIN_CONFIG_PREFIX + "autoCertRefreshTime")
                                        .defaultValue(300000);
}
