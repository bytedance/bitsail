/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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

import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.function.Supplier;

public class KuduWriter<CommitT> implements Writer<Row, CommitT, EmptyState> {
  private static final Logger LOG = LoggerFactory.getLogger(KuduWriter.class);

  private final String kuduTableName;

  private final KuduClient kuduClient;
  private final KuduSession kuduSession;
  private final KuduTable kuduTable;

  private final Supplier<Operation> operationSupplier;

  public KuduWriter(BitSailConfiguration jobConf) {
    this.kuduTableName = jobConf.getNecessaryOption(KuduWriterOptions.KUDU_TABLE_NAME, KuduErrorCode.REQUIRED_VALUE);

    KuduFactory factory = new KuduFactory(jobConf, "writer");
    this.kuduClient = factory.getClient();
    this.kuduSession = factory.getSession();

    try {
      this.kuduTable = kuduClient.openTable(kuduTableName);
    } catch (KuduException e) {
      LOG.error("Failed to open table {}.", kuduTableName, e);
      throw new BitSailException(KuduErrorCode.OPEN_TABLE_ERROR, e.getMessage());
    }

    String writeMode = jobConf.get(KuduWriterOptions.WRITE_MODE).trim().toUpperCase();
    this.operationSupplier = initOperationSupplier(writeMode);
  }

  @Override
  public void write(Row element) throws IOException {

  }

  private Supplier<Operation> initOperationSupplier(String writeMode) {
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
}
