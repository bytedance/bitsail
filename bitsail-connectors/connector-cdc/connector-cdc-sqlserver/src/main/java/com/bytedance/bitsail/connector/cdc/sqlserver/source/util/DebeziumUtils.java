/*
 * Copyright 2022-2023 Bytedance Ltd. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.bytedance.bitsail.connector.cdc.sqlserver.source.util;

import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.connector.cdc.error.BinlogReaderErrorCode;
import com.bytedance.bitsail.connector.cdc.source.offset.BinlogOffset;
import com.bytedance.bitsail.connector.cdc.source.split.BinlogSplit;
import com.bytedance.bitsail.connector.cdc.sqlserver.source.config.SqlServerConfig;

import io.debezium.connector.sqlserver.Lsn;
import io.debezium.connector.sqlserver.SqlServerConnection;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig;
import io.debezium.connector.sqlserver.SqlServerOffsetContext;
import io.debezium.connector.sqlserver.TxLogPosition;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotContext;
import io.debezium.pipeline.txmetadata.TransactionContext;

import java.sql.SQLException;
import java.util.Map;

public class DebeziumUtils {
  public static SqlServerOffsetContext loadOffsetContext(SqlServerConnectorConfig connectorConfig,
                                                         BinlogSplit split, SqlServerConnection connection) throws SQLException {
    final SqlServerOffsetContext offsetContext;
    switch (split.getBeginOffset().getOffsetType()) {
      case EARLIEST:
        offsetContext = new SqlServerOffsetContext(connectorConfig, TxLogPosition.valueOf(connection.getNthTransactionLsnFromBeginning(0)), false, false);
        break;
      case LATEST:
        offsetContext = new SqlServerOffsetContext(connectorConfig, TxLogPosition.valueOf(connection.getMaxLsn()), false, false);
        break;
      case SPECIFIED:
        BinlogOffset offset = split.getBeginOffset();
        final Lsn changeLsn = Lsn.valueOf(offset.getProps().get(SqlServerConfig.CHANGE_LSN));
        final Lsn commitLsn = Lsn.valueOf(offset.getProps().get(SqlServerConfig.COMMIT_LSN));
        long eventSerialNo = Long.parseLong(offset.getProps().get(SqlServerConfig.EVENT_SERIAL_NO));
        offsetContext = new SqlServerOffsetContext(connectorConfig,
            TxLogPosition.valueOf(commitLsn, changeLsn), false, false, eventSerialNo,
            new TransactionContext(), new IncrementalSnapshotContext<>());
        break;
      default:
        throw new BitSailException(BinlogReaderErrorCode.UNSUPPORTED_ERROR,
            String.format("the begin binlog type %s is not supported", split.getBeginOffset().getOffsetType()));
    }
    return offsetContext;
  }

  public static BinlogOffset convertDbzOffsetToBinlogOffset(Map<String, String> dbzOffset) {
    BinlogOffset offset = BinlogOffset.specified();
    offset.addProps(SqlServerConfig.CHANGE_LSN, dbzOffset.get(SqlServerConfig.CHANGE_LSN));
    offset.addProps(SqlServerConfig.COMMIT_LSN, dbzOffset.get(SqlServerConfig.COMMIT_LSN));
    offset.addProps(SqlServerConfig.EVENT_SERIAL_NO, dbzOffset.get(SqlServerConfig.EVENT_SERIAL_NO));
    return offset;
  }
}
