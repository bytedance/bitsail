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

package com.bytedance.bitsail.connector.kudu.core.config;

import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.exception.CommonErrorCode;
import com.bytedance.bitsail.common.option.ConfigOption;
import com.bytedance.bitsail.connector.kudu.error.KuduErrorCode;
import com.bytedance.bitsail.connector.kudu.option.KuduReaderOptions;
import com.bytedance.bitsail.connector.kudu.option.KuduWriterOptions;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.kudu.client.AsyncKuduClient;

import java.io.Serializable;
import java.util.List;

@Getter
public class KuduClientConfig implements Serializable {

  /**
   * reader or writer.
   */
  private final String role;

  private final List<String> masterAddressList;

  private final long adminOperationTimeout;
  private final long operationTimeout;
  private final long connectionNegotiationTimeout;

  private final Boolean disableClientStatistics;
  private final Integer workerCount;
  private final String saslProtocolName;
  private final Boolean requireAuthentication;
  private final AsyncKuduClient.EncryptionPolicy encryptionPolicy;

  /**
   * Init kudu client configurations.
   * @param jobConf Job configuration.
   * @param role Init kudu client from reader or writer.
   */
  public KuduClientConfig(BitSailConfiguration jobConf, String role) {
    this.role = role.trim().toUpperCase();

    this.masterAddressList = getOption(ClientOption.MASTER_ADDRESS_LIST, jobConf);

    this.adminOperationTimeout = getOption(ClientOption.ADMIN_OPERATION_TIMEOUT_MS, jobConf);
    this.operationTimeout = getOption(ClientOption.OPERATION_TIMEOUT_MS, jobConf);
    this.connectionNegotiationTimeout = getOption(ClientOption.CONNECTION_NEGOTIATION_TIMEOUT_MS, jobConf);

    this.disableClientStatistics = getOption(ClientOption.DISABLE_CLIENT_STATISTICS, jobConf);
    this.workerCount = getOption(ClientOption.WORKER_COUNT, jobConf);
    this.saslProtocolName = getOption(ClientOption.SASL_PROTOCOL_NAME, jobConf);
    this.requireAuthentication = getOption(ClientOption.REQUIRE_AUTHENTICATION, jobConf);

    String encPolicy = getOption(ClientOption.ENCRYPTION_POLICY, jobConf);
    this.encryptionPolicy = StringUtils.isEmpty(encPolicy) ? null : AsyncKuduClient.EncryptionPolicy.valueOf(encPolicy);

    validate();
  }

  /**
   * Check if configurations are valid.
   */
  public void validate() {
    if (masterAddressList == null || masterAddressList.isEmpty()) {
      throw new BitSailException(KuduErrorCode.CONFIG_ERROR, KuduWriterOptions.MASTER_ADDRESS_LIST.key() + " cannot be empty");
    }
  }

  @AllArgsConstructor
  enum ClientOption {
    MASTER_ADDRESS_LIST(KuduReaderOptions.MASTER_ADDRESS_LIST, KuduWriterOptions.MASTER_ADDRESS_LIST),
    ADMIN_OPERATION_TIMEOUT_MS(KuduReaderOptions.ADMIN_OPERATION_TIMEOUT_MS, KuduWriterOptions.ADMIN_OPERATION_TIMEOUT_MS),
    OPERATION_TIMEOUT_MS(KuduReaderOptions.OPERATION_TIMEOUT_MS, KuduWriterOptions.OPERATION_TIMEOUT_MS),
    CONNECTION_NEGOTIATION_TIMEOUT_MS(KuduReaderOptions.CONNECTION_NEGOTIATION_TIMEOUT_MS, KuduWriterOptions.CONNECTION_NEGOTIATION_TIMEOUT_MS),
    DISABLE_CLIENT_STATISTICS(KuduReaderOptions.DISABLE_CLIENT_STATISTICS, KuduWriterOptions.DISABLE_CLIENT_STATISTICS),
    WORKER_COUNT(KuduReaderOptions.WORKER_COUNT, KuduWriterOptions.WORKER_COUNT),
    SASL_PROTOCOL_NAME(KuduReaderOptions.SASL_PROTOCOL_NAME, KuduWriterOptions.SASL_PROTOCOL_NAME),
    REQUIRE_AUTHENTICATION(KuduReaderOptions.REQUIRE_AUTHENTICATION, KuduWriterOptions.REQUIRE_AUTHENTICATION),
    ENCRYPTION_POLICY(KuduReaderOptions.ENCRYPTION_POLICY, KuduWriterOptions.ENCRYPTION_POLICY);

    private final ConfigOption<?> readerOption;
    private final ConfigOption<?> writerOption;

    public Object get(BitSailConfiguration jobConf, String role) {
      switch (role) {
        case "READER":
          return jobConf.get(readerOption);
        case "WRITER":
          return jobConf.get(writerOption);
        default:
          throw new BitSailException(CommonErrorCode.INTERNAL_ERROR, "Kudu client option error, please check codes!");
      }
    }
  }

  private  <T> T getOption(ClientOption option, BitSailConfiguration jobConf) {
    return (T) option.get(jobConf, role);
  }
}
