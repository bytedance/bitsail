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

package com.bytedance.bitsail.connector.kudu.core;

import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.util.Preconditions;
import com.bytedance.bitsail.connector.kudu.core.config.KuduClientConfig;
import com.bytedance.bitsail.connector.kudu.core.config.KuduSessionConfig;
import com.bytedance.bitsail.connector.kudu.error.KuduErrorCode;

import org.apache.kudu.Schema;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;

/**
 * A factory to manage kudu client and kudu session.
 */
public class KuduFactory implements Closeable, Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(KuduFactory.class);

  private final KuduClientConfig clientConfig;
  private final KuduSessionConfig sessionConfig;

  private transient KuduClient kuduClient;
  private transient KuduSession kuduSession;

  /**
   * Init factory for KuduSource.
   */
  public static KuduFactory initReaderFactory(BitSailConfiguration jobConf) {
    return new KuduFactory(jobConf, "reader");
  }

  /**
   * Init factory for KuduSink.
   */
  public static KuduFactory initWriterFactory(BitSailConfiguration jobConf) {
    return new KuduFactory(jobConf, "writer");
  }

  /**
   * Init a kudu factory.
   */
  private KuduFactory(BitSailConfiguration jobConf, String role) {
    // initialize client config
    role = role.trim();
    Preconditions.checkState("READER".equalsIgnoreCase(role) || "WRITER".equalsIgnoreCase(role));
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
    LOG.info("Kudu client is initialized.");

    return kuduClient;
  }

  /**
   * Make sure there is only one active session in any moment.
   */
  public KuduSession getSession() {
    if (kuduSession == null || kuduSession.isClosed()) {
      kuduSession = getClient().newSession();
      kuduSession.setFlushMode(sessionConfig.getFlushMode());
      kuduSession.setExternalConsistencyMode(sessionConfig.getConsistencyMode());

      if (sessionConfig.getMutationBufferSize() != null) {
        kuduSession.setMutationBufferSpace(sessionConfig.getMutationBufferSize());
        kuduSession.setErrorCollectorSpace(sessionConfig.getMutationBufferSize());
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
      LOG.info("Kudu session is created.");
    }

    return kuduSession;
  }

  /**
   * Get specific table form current client.
   */
  public KuduTable getTable(String tableName) throws BitSailException {
    try {
      return getClient().openTable(tableName);
    } catch (KuduException e) {
      LOG.error("Failed to open table {}.", tableName, e);
      throw new BitSailException(KuduErrorCode.OPEN_TABLE_ERROR, e.getMessage());
    }
  }

  /**
   * Get schema of specific table.
   */
  public Schema getSchema(String tableName) throws BitSailException {
    return getTable(tableName).getSchema();
  }

  /**
   * Close current kudu client.
   */
  public void closeCurrentClient() throws IOException {
    if (kuduClient != null) {
      try {
        kuduClient.shutdown();
      } catch (KuduException e) {
        throw new IOException("Failed to close kudu client.", e);
      } finally {
        kuduClient = null;
      }
      LOG.info("Current kudu client is closed.");
    }
  }

  /**
   * Close current kudu session.
   */
  public void closeCurrentSession() throws IOException {
    if (kuduSession != null && !kuduSession.isClosed()) {
      try {
        kuduSession.close();
      } catch (KuduException e) {
        throw new IOException("Failed to close kudu session.", e);
      } finally {
        kuduSession = null;
      }
      LOG.info("Current kudu session is closed.");
    }
  }

  /**
   * Close factory, including kudu client and kudu session.
   * @throws IOException
   */
  @Override
  public void close() throws IOException {
    closeCurrentSession();
    closeCurrentClient();
  }
}
