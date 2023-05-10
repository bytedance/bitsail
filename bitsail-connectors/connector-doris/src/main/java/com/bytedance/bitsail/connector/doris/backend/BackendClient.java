/*
 * Copyright 2022 Bytedance Ltd. and/or its affiliates.
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

package com.bytedance.bitsail.connector.doris.backend;

import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.connector.doris.backend.model.Routing;
import com.bytedance.bitsail.connector.doris.config.DorisExecutionOptions;
import com.bytedance.bitsail.connector.doris.error.DorisErrorCode;

import org.apache.doris.sdk.thrift.TDorisExternalService;
import org.apache.doris.sdk.thrift.TScanBatchResult;
import org.apache.doris.sdk.thrift.TScanCloseParams;
import org.apache.doris.sdk.thrift.TScanCloseResult;
import org.apache.doris.sdk.thrift.TScanNextBatchParams;
import org.apache.doris.sdk.thrift.TScanOpenParams;
import org.apache.doris.sdk.thrift.TScanOpenResult;
import org.apache.doris.sdk.thrift.TStatusCode;
import org.apache.thrift.TConfiguration;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Client to request Doris BE
 */
public class BackendClient {
  private static final Logger LOGGER = LoggerFactory.getLogger(BackendClient.class);
  private final Routing routing;
  private TDorisExternalService.Client client;
  private TTransport transport;
  private boolean isConnected = false;
  private final int retries;
  private final int socketTimeout;
  private final int connectTimeout;

  public BackendClient(Routing routing, DorisExecutionOptions executionOptions) {
    this.routing = routing;
    this.connectTimeout = executionOptions.getRequestConnectTimeoutMs();
    this.socketTimeout = executionOptions.getRequestReadTimeoutMs();
    this.retries = executionOptions.getRequestRetries();
    LOGGER.trace("connect timeout set to '{}'. socket timeout set to '{}'. retries set to '{}'.",
        this.connectTimeout, this.socketTimeout, this.retries);
    open();
  }

  private void open() {
    LOGGER.debug("Open client to Doris BE '{}'.", routing);
    TException ex = null;
    for (int attempt = 0; !isConnected && attempt < retries; ++attempt) {
      LOGGER.debug("Attempt {} to connect {}.", attempt, routing);
      try {
        TBinaryProtocol.Factory factory = new TBinaryProtocol.Factory();
        transport = new TSocket(new TConfiguration(), routing.getHost(), routing.getPort(), socketTimeout, connectTimeout);
        TProtocol protocol = factory.getProtocol(transport);
        client = new TDorisExternalService.Client(protocol);
        if (isConnected) {
          LOGGER.info("Success connect to {}.", routing);
          return;
        }
        LOGGER.trace("Connect status before open transport to {} is '{}'.", routing, isConnected);
        if (!transport.isOpen()) {
          transport.open();
          isConnected = true;
        }
      } catch (TTransportException e) {
        LOGGER.warn("Failed to connect message, routing={}", routing, e);
        ex = e;
      }
    }
    if (!isConnected) {
      String errMsg = String.format("Failed to connect message, routing=%s", routing);
      LOGGER.error(errMsg, ex);
      throw BitSailException.asBitSailException(DorisErrorCode.CONNECT_FAILED_MESSAGE, errMsg, ex);
    }
  }

  private void close() {
    LOGGER.trace("Connect status before close with '{}' is '{}'.", routing, isConnected);
    isConnected = false;
    if ((transport != null) && transport.isOpen()) {
      transport.close();
      LOGGER.info("Closed a connection to {}.", routing);
    }
    if (null != client) {
      client = null;
    }
  }

  /**
   * Open a scanner for reading Doris data.
   *
   * @param openParams thrift struct to required by request
   * @return scan open result
   */
  public TScanOpenResult openScanner(TScanOpenParams openParams) {
    LOGGER.debug("OpenScanner to '{}', parameter is '{}'.", routing, openParams);
    if (!isConnected) {
      open();
    }
    TException ex = null;
    for (int attempt = 0; attempt < retries; ++attempt) {
      LOGGER.debug("Attempt {} to openScanner {}.", attempt, routing);
      try {
        TScanOpenResult result = client.openScanner(openParams);
        if (result == null) {
          LOGGER.warn("Open scanner result from {} is null.", routing);
          continue;
        }
        if (!TStatusCode.OK.equals(result.getStatus().getStatusCode())) {
          LOGGER.warn("The status of open scanner result from {} is '{}', error message is: {}.",
              routing, result.getStatus().getStatusCode(), result.getStatus().getErrorMsgs());
          continue;
        }
        return result;
      } catch (TException e) {
        LOGGER.warn("Open scanner from {} failed.", routing, e);
        ex = e;
      }
    }
    String errMsg = String.format("Failed to connect message, routing=%s", routing);
    LOGGER.error(errMsg, ex);
    throw BitSailException.asBitSailException(DorisErrorCode.CONNECT_FAILED_MESSAGE, errMsg, ex);
  }

  /**
   * get next row batch from Doris BE
   *
   * @param nextBatchParams thrift struct to required by request
   * @return scan batch result
   */
  public TScanBatchResult getNext(TScanNextBatchParams nextBatchParams) {
    LOGGER.debug("GetNext to '{}', parameter is '{}'.", routing, nextBatchParams);
    if (!isConnected) {
      open();
    }
    TException ex = null;
    TScanBatchResult result = null;
    for (int attempt = 0; attempt < retries; ++attempt) {
      LOGGER.debug("Attempt {} to getNext {}.", attempt, routing);
      try {
        result = client.getNext(nextBatchParams);
        if (result == null) {
          LOGGER.warn("GetNext result from {} is null.", routing);
          continue;
        }
        if (!TStatusCode.OK.equals(result.getStatus().getStatusCode())) {
          LOGGER.warn("The status of get next result from {} is '{}', error message is: {}.",
              routing, result.getStatus().getStatusCode(), result.getStatus().getErrorMsgs());
          continue;
        }
        return result;
      } catch (TException e) {
        LOGGER.warn("Get next from {} failed.", routing, e);
        ex = e;
      }
    }
    if (result != null && (TStatusCode.OK != (result.getStatus().getStatusCode()))) {
      String errMsg = String.format("Doris Internal error, routing=%s, status=%s, errorMsgs=%s",
          routing, result.getStatus().getStatusCode(), result.getStatus().getErrorMsgs());
      LOGGER.error(errMsg);
      throw BitSailException.asBitSailException(DorisErrorCode.INTERNAL_FAIL_MESSAGE, errMsg);
    }
    String errMsg = String.format("Failed to connect message, routing=%s", routing);
    LOGGER.error(errMsg);
    throw BitSailException.asBitSailException(DorisErrorCode.CONNECT_FAILED_MESSAGE, errMsg, ex);
  }

  /**
   * close an scanner.
   *
   * @param closeParams thrift struct to required by request
   */
  public void closeScanner(TScanCloseParams closeParams) {
    LOGGER.debug("CloseScanner to '{}', parameter is '{}'.", routing, closeParams);
    for (int attempt = 0; attempt < retries; ++attempt) {
      LOGGER.debug("Attempt {} to closeScanner {}.", attempt, routing);
      try {
        TScanCloseResult result = client.closeScanner(closeParams);
        if (result == null) {
          LOGGER.warn("CloseScanner result from {} is null.", routing);
          continue;
        }
        if (!TStatusCode.OK.equals(result.getStatus().getStatusCode())) {
          LOGGER.warn("The status of get next result from {} is '{}', error message is: {}.",
              routing, result.getStatus().getStatusCode(), result.getStatus().getErrorMsgs());
          continue;
        }
        break;
      } catch (TException e) {
        LOGGER.warn("Close scanner from {} failed.", routing, e);
      }
    }
    LOGGER.info("CloseScanner to Doris BE '{}' success.", routing);
    close();
  }
}
