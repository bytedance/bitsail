/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.bytedance.bitsail.connector.kudu.core;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.util.Preconditions;
import com.bytedance.bitsail.connector.kudu.config.KuduClientConfig;
import com.bytedance.bitsail.connector.kudu.config.KuduSessionConfig;

import lombok.Data;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduSession;

import java.io.Serializable;

@Data
public class KuduFactory implements Serializable {

  private final KuduClientConfig clientConfig;
  private final KuduSessionConfig sessionConfig;

  private KuduClient kuduClient;
  private KuduSession kuduSession;

  public KuduFactory(BitSailConfiguration jobConf, String role) {
    // initialize client config
    role = role.trim();
    Preconditions.checkState("reader".equalsIgnoreCase(role) || "writer".equalsIgnoreCase(role));
    this.clientConfig = new KuduClientConfig(jobConf, role);
    clientConfig.validate();

    // initialize session config
    this.sessionConfig = new KuduSessionConfig(jobConf);
    sessionConfig.validate();
  }

  /**
   * Make sure one KuduFactory only creates one client.
   */
  public KuduClient getClient() {
    if (kuduClient != null) {
      return kuduClient;
    }

    KuduClient.KuduClientBuilder builder = new KuduClient.KuduClientBuilder(clientConfig.getMasterAddressList());

    builder.defaultAdminOperationTimeoutMs(clientConfig.getAdminOperationTimeout());
    builder.defaultOperationTimeoutMs(clientConfig.getAdminOperationTimeout());
    builder.connectionNegotiationTimeoutMs(clientConfig.getConnectionNegotiationTimeout());

    if (clientConfig.getDisableClientStatistics() != null && clientConfig.getDisableClientStatistics()) {
      builder.disableStatistics();
    }

    if (clientConfig.getWorkerCount() != null) {
      builder.workerCount(clientConfig.getWorkerCount());
    }

    if (clientConfig.getSaslProtocolName() != null) {
      builder.saslProtocolName(clientConfig.getSaslProtocolName());
    }

    if (clientConfig.getRequireAuthentication() != null) {
      builder.requireAuthentication(clientConfig.getRequireAuthentication());
    }

    if (clientConfig.getEncryptionPolicy() != null) {
      builder.encryptionPolicy(clientConfig.getEncryptionPolicy());
    }

    this.kuduClient = builder.build();
    return kuduClient;
  }

  /**
   * Make sure one KuduFactory only creates one session.
   */
  public KuduSession getSession() {
    // Make sure KuduClient is initialized.
     getClient();

    if (kuduSession == null) {
      kuduSession = kuduClient.newSession();
      kuduSession.setFlushMode(sessionConfig.getFlushMode());
      kuduSession.setExternalConsistencyMode(sessionConfig.getConsistencyMode());

      if (sessionConfig.getMutationBufferSize() != null) {
        kuduSession.setMutationBufferSpace(sessionConfig.getMutationBufferSize());
      }

      if (sessionConfig.getFlushInterval() != null) {
        kuduSession.setFlushInterval(sessionConfig.getFlushInterval());
      }

      if (sessionConfig.getTimeout() != null) {
        kuduSession.setTimeoutMillis(sessionConfig.getTimeout());
      }

      if (sessionConfig.getIgnoreDuplicateRows() != null) {
        kuduSession.setIgnoreAllDuplicateRows(sessionConfig.getIgnoreDuplicateRows());
      }
    }

    return kuduSession;
  }
}
