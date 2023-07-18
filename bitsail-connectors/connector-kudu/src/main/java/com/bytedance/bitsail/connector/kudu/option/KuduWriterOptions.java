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
import com.bytedance.bitsail.common.option.WriterOptions;

import com.alibaba.fastjson.TypeReference;

import java.util.List;

import static com.bytedance.bitsail.common.option.ConfigOptions.key;
import static com.bytedance.bitsail.common.option.WriterOptions.WRITER_PREFIX;

public interface KuduWriterOptions extends WriterOptions.BaseWriterOptions {
  @Essential
  ConfigOption<String> KUDU_TABLE_NAME =
      key(WRITER_PREFIX + "kudu_table_name")
          .noDefaultValue(String.class);

  @Essential
  ConfigOption<List<String>> MASTER_ADDRESS_LIST =
      key(WRITER_PREFIX + "kudu_master_address_list")
          .onlyReference(new TypeReference<List<String>>() {});

  ConfigOption<String> WRITE_MODE =
      key(WRITER_PREFIX + "kudu_write_mode")
          .defaultValue("INSERT");

  // Belows are kudu client timeout options with default values.

  ConfigOption<Long> ADMIN_OPERATION_TIMEOUT_MS =
      key(WRITER_PREFIX + "kudu_admin_operation_timeout_ms")
          .defaultValue(30000L);

  ConfigOption<Long> OPERATION_TIMEOUT_MS =
      key(WRITER_PREFIX + "kudu_operation_timeout_ms")
          .defaultValue(30000L);

  ConfigOption<Long> CONNECTION_NEGOTIATION_TIMEOUT_MS =
      key(WRITER_PREFIX + "kudu_connection_negotiation_timeout_ms")
          .defaultValue(10000L);

  // Belows are other kudu client options.
  ConfigOption<Boolean> DISABLE_CLIENT_STATISTICS =
      key(WRITER_PREFIX + "kudu_disable_client_statistics")
          .noDefaultValue(Boolean.class);

  /**
   * If not set, use the kudu default value: 2 * Runtime.getRuntime().availableProcessors()
   */
  ConfigOption<Integer> WORKER_COUNT =
      key(WRITER_PREFIX + "kudu_worker_count")
          .noDefaultValue(Integer.class);

  /**
   * If not set, use the kudu default value: "kudu"
   */
  ConfigOption<String> SASL_PROTOCOL_NAME =
      key(WRITER_PREFIX + "kudu_sasl_protocol_name")
          .noDefaultValue(String.class);

  /**
   * If not set, use the kudu default value: false
   */
  ConfigOption<Boolean> REQUIRE_AUTHENTICATION =
      key(WRITER_PREFIX + "kudu_require_authentication")
          .noDefaultValue(Boolean.class);

  /**
   * If not set, use the kudu default: OPTIONAL.<br/>
   * Ref: {@link org.apache.kudu.client.AsyncKuduClient.EncryptionPolicy}
   */
  ConfigOption<String> ENCRYPTION_POLICY =
      key(WRITER_PREFIX + "kudu_encryption_policy")
          .noDefaultValue(String.class);

  // Belows are KuduSession options.
  /**
   * Flush mode used by {@link org.apache.kudu.client.KuduSession}.<br/>
   * Ref: {@link org.apache.kudu.client.SessionConfiguration.FlushMode}
   */
  ConfigOption<String> SESSION_FLUSH_MODE =
      key(WRITER_PREFIX + "kudu_session_flush_mode")
          .defaultValue("AUTO_FLUSH_BACKGROUND");

  /**
   * The number of operations that can be buffered.
   */
  ConfigOption<Integer> MUTATION_BUFFER_SIZE =
      key(WRITER_PREFIX + "kudu_mutation_buffer_size")
          .defaultValue(1024);

  ConfigOption<Integer> SESSION_FLUSH_INTERVAL =
      key(WRITER_PREFIX + "kudu_session_flush_interval")
          .noDefaultValue(Integer.class);

  /**
   * Sets the timeout for operations.
   * The default timeout is 0, which disables the timeout functionality.
   */
  ConfigOption<Long> SESSION_TIMEOUT_MS =
      key(WRITER_PREFIX + "kudu_session_timeout_ms")
          .noDefaultValue(Long.class);

  /**
   * External consistency mode for kudu session.<br/>
   * Ref: {@link org.apache.kudu.client.ExternalConsistencyMode}
   */
  ConfigOption<String> SESSION_EXTERNAL_CONSISTENCY_MODE =
      key(WRITER_PREFIX + "kudu_session_external_consistency_mode")
          .defaultValue("CLIENT_PROPAGATED");

  /**
   * Whether ignore all the row errors if they are all of the AlreadyPresent type.
   * Throw exceptions if false.
   */
  ConfigOption<Boolean> IGNORE_ALL_DUPLICATE_ROWS =
      key(WRITER_PREFIX + "kudu_ignore_duplicate_rows")
          .defaultValue(false);
}
