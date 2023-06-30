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

package com.bytedance.bitsail.connector.kudu.option;

import com.bytedance.bitsail.common.annotation.Essential;
import com.bytedance.bitsail.common.option.ConfigOption;
import com.bytedance.bitsail.common.option.ReaderOptions;

import com.alibaba.fastjson.TypeReference;

import java.util.List;

import static com.bytedance.bitsail.common.option.ConfigOptions.key;
import static com.bytedance.bitsail.common.option.ReaderOptions.READER_PREFIX;

public interface KuduReaderOptions extends ReaderOptions.BaseReaderOptions {
  @Essential
  ConfigOption<String> KUDU_TABLE_NAME =
      key(READER_PREFIX + "kudu_table_name")
          .noDefaultValue(String.class);

  @Essential
  ConfigOption<List<String>> MASTER_ADDRESS_LIST =
      key(READER_PREFIX + "kudu_master_address_list")
          .onlyReference(new TypeReference<List<String>>() {});

  // Belows are kudu client timeout options with default values.

  ConfigOption<Long> ADMIN_OPERATION_TIMEOUT_MS =
      key(READER_PREFIX + "kudu_admin_operation_timeout_ms")
          .defaultValue(30000L);

  ConfigOption<Long> OPERATION_TIMEOUT_MS =
      key(READER_PREFIX + "kudu_operation_timeout_ms")
          .defaultValue(30000L);

  ConfigOption<Long> CONNECTION_NEGOTIATION_TIMEOUT_MS =
      key(READER_PREFIX + "kudu_connection_negotiation_timeout_ms")
          .defaultValue(10000L);

  // Belows are other kudu client options.

  ConfigOption<Boolean> DISABLE_CLIENT_STATISTICS =
      key(READER_PREFIX + "kudu_disable_client_statistics")
          .noDefaultValue(Boolean.class);

  /**
   * If not set, use the kudu default value: 2 * Runtime.getRuntime().availableProcessors()
   */
  ConfigOption<Integer> WORKER_COUNT =
      key(READER_PREFIX + "kudu_worker_count")
          .noDefaultValue(Integer.class);

  /**
   * If not set, use the kudu default value: "kudu"
   */
  ConfigOption<String> SASL_PROTOCOL_NAME =
      key(READER_PREFIX + "sasl_protocol_name")
          .noDefaultValue(String.class);

  /**
   * If not set, use the kudu default value: false
   */
  ConfigOption<Boolean> REQUIRE_AUTHENTICATION =
      key(READER_PREFIX + "require_authentication")
          .noDefaultValue(Boolean.class);

  /**
   * If not set, use the kudu default: OPTIONAL.<br/>
   * Ref: {@link org.apache.kudu.client.AsyncKuduClient.EncryptionPolicy}
   */
  ConfigOption<String> ENCRYPTION_POLICY =
      key(READER_PREFIX + "encryption_policy")
          .noDefaultValue(String.class);

  // Belows are scan token options.
  /**
   * Ref: {@link org.apache.kudu.client.AsyncKuduScanner.ReadMode}
   */
  ConfigOption<String> READ_MODE =
      key(READER_PREFIX + "read_mode")
          .defaultValue("READ_LATEST");

  ConfigOption<Long> SNAPSHOT_TIMESTAMP_US =
      key(READER_PREFIX + "snapshot_timestamp_us")
          .noDefaultValue(Long.class);

  ConfigOption<Boolean> FAULT_TOLERANT =
      key(READER_PREFIX + "enable_fault_tolerant")
      .defaultValue(false);

  ConfigOption<Integer> SCAN_BATCH_SIZE_BYTES =
      key(READER_PREFIX + "scan_batch_size_bytes")
          .noDefaultValue(Integer.class);

  ConfigOption<Long> SCAN_MAX_COUNT =
      key(READER_PREFIX + "scan_max_count")
          .noDefaultValue(Long.class);

  ConfigOption<Boolean> CACHE_BLOCKS =
      key(READER_PREFIX + "enable_cache_blocks")
          .defaultValue(false);

  ConfigOption<Long> SCAN_TIMEOUT =
      key(READER_PREFIX + "scan_timeout_ms")
          .defaultValue(30000L);
  ConfigOption<Long> SCAN_ALIVE_PERIOD_MS =
      key(READER_PREFIX + "scan_keep_alive_period_ms")
          .noDefaultValue(Long.class);
  ConfigOption<Long> SCAN_SPLIT_SIZE_BYTES =
      key(READER_PREFIX + "scan_split_size_bytes")
          .defaultValue(-1L);
  ConfigOption<String> PREDICATES_CONFIGURATION =
      key(READER_PREFIX + "predicates")
          .noDefaultValue(String.class);
}
