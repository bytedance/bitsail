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

package com.bytedance.bitsail.connector.kudu.sink;

import com.bytedance.bitsail.base.connector.writer.v1.Writer;
import com.bytedance.bitsail.base.connector.writer.v1.state.EmptyState;
import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.connector.kudu.core.KuduFactory;
import com.bytedance.bitsail.connector.kudu.error.KuduErrorCode;
import com.bytedance.bitsail.connector.kudu.option.KuduWriterOptions;

import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;
import org.apache.kudu.client.OperationResponse;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.RowError;
import org.apache.kudu.client.RowErrorsAndOverflowStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

public class KuduWriter<CommitT> implements Writer<Row, CommitT, EmptyState> {
  private static final Logger LOG = LoggerFactory.getLogger(KuduWriter.class);
  private static final int SESSION_ERROR_LOG_COUNT = 5;

  private final KuduFactory kuduFactory;
  private final transient KuduTable kuduTable;
  private transient KuduSession kuduSession;

  private final transient Supplier<Operation> operationSupplier;

  private final transient KuduRowBuilder rowBuilder;

  public KuduWriter(BitSailConfiguration jobConf) {
    String kuduTableName = jobConf.getNecessaryOption(KuduWriterOptions.KUDU_TABLE_NAME, KuduErrorCode.REQUIRED_VALUE);

    this.kuduFactory = KuduFactory.initWriterFactory(jobConf);
    this.kuduSession = kuduFactory.getSession();
    this.kuduTable = kuduFactory.getTable(kuduTableName);

    String writeMode = jobConf.get(KuduWriterOptions.WRITE_MODE);
    this.operationSupplier = initOperationSupplier(writeMode);
    this.rowBuilder = new KuduRowBuilder(jobConf, kuduTable.getSchema());

    LOG.info("KuduWriter is initialized.");
  }

  @Override
  public void write(Row element) throws IOException {
    Operation operation = operationSupplier.get();
    PartialRow kuduRow = operation.getRow();
    rowBuilder.build(kuduRow, element);

    OperationResponse response;
    try {
      response = kuduSession.apply(operation);
    } catch (KuduException e) {
      e.printStackTrace();
      throw new IOException("Failed to write element to session.");
    }

    handleOperationResponse(response);
    // if back_ground_flush need check
    handleSessionPendingErrors(kuduSession);
  }

  @Override
  public void flush(boolean endOfInput) throws IOException {
    List<OperationResponse> responses;
    try {
      responses = kuduSession.flush();
    } catch (KuduException e) {
      LOG.error("Failed to flush rows in session.", e);
      throw new IOException("Failed to flush rows in session.", e);
    }

    // Check if any error occurs.
    for (OperationResponse response : responses) {
      handleOperationResponse(response);
    }
    handleSessionPendingErrors(kuduSession);
  }

  @Override
  public List<CommitT> prepareCommit() {
    return Collections.emptyList();
  }

  @Override
  public List<EmptyState> snapshotState(long checkpointId) throws IOException {
    this.flush(false);
    return Collections.emptyList();
  }

  @Override
  public void close() throws IOException {
    flush(true);

    kuduFactory.closeCurrentSession();
    handleSessionPendingErrors(kuduSession);
    kuduFactory.close();
  }

  /**
   * Create a supplier which get operations from table according to write mode.
   */
  private Supplier<Operation> initOperationSupplier(String writeMode) {
    writeMode = writeMode.trim().toUpperCase();
    switch (writeMode) {
      case "INSERT":
        return kuduTable::newInsert;
      case "INSERT_IGNORE":
        return kuduTable::newInsertIgnore;
      case "UPSERT":
        return kuduTable::newUpsert;
      case "UPDATE":
        return kuduTable::newUpdate;
      case "UPDATE_IGNORE":
        return kuduTable::newUpdateIgnore;
      case "DELETE":
        return kuduTable::newDelete;
      case "DELETE_IGNORE":
        return kuduTable::newDeleteIgnore;
      default:
        throw new BitSailException(KuduErrorCode.UNSUPPORTED_OPERATION, "Write mode " + writeMode + " is not supported.");
    }
  }

  /**
   * Check if an operation is failed.
   */
  private static void handleOperationResponse(OperationResponse operationResponse) throws IOException {
    if (operationResponse != null && operationResponse.hasRowError()) {
      LOG.error("Failed to add an operation to session: {}", operationResponse.getRowError());
      throw new IOException("Failed to add an operation to session: " + operationResponse.getRowError());
    }
  }

  /**
   * Check if an session has errors.
   */
  private static void handleSessionPendingErrors(KuduSession session) throws IOException {
    if (session != null && session.countPendingErrors() != 0) {
      RowErrorsAndOverflowStatus roStatus = session.getPendingErrors();
      RowError[] errs = roStatus.getRowErrors();

      LOG.error("Found {} pending errors.", session.countPendingErrors());
      if (roStatus.isOverflowed()) {
        LOG.error("error buffer overflowed: some errors were discarded");
      }

      List<String> sessionErrorMessage = new ArrayList<>();
      for (int i = 0; i < errs.length; ++i) {
        LOG.error("The {}-th error: {}", i, errs[i]);
        if (i < SESSION_ERROR_LOG_COUNT) {
          sessionErrorMessage.add(String.format("[error-%d: %s]", i, errs[i]));
        }
      }

      throw new IOException("Found " + session.countPendingErrors() + " errors in session. The first few errors are: " + String.join(", ", sessionErrorMessage));
    }
  }
}
