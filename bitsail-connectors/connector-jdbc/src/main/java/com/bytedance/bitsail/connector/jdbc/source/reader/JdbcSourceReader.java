/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.bytedance.bitsail.connector.jdbc.source.reader;

import com.bytedance.bitsail.base.connector.reader.v1.SourcePipeline;
import com.bytedance.bitsail.base.connector.reader.v1.SourceReader;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.model.ColumnInfo;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.connector.jdbc.error.JdbcErrorCode;
import com.bytedance.bitsail.connector.jdbc.option.JdbcReaderOptions;
import com.bytedance.bitsail.connector.jdbc.source.split.JdbcSourceSplit;
import com.bytedance.bitsail.connector.jdbc.util.JdbcConnectionHolder;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;

@Slf4j
public class JdbcSourceReader implements SourceReader<Row, JdbcSourceSplit> {

  private final int subTaskId;
  private int totalSplitNum = 0;
  private boolean hasNoMoreSplits = false;
  private final Deque<JdbcSourceSplit> splits;

  private final List<ColumnInfo> columnInfos;
  private final String dbName;
  private final String tableName;
  private final String splitField;

  private final String filterSql;
  private final Long maxFetchCount;

  private final transient JdbcConnectionHolder connectionHolder;
  private final transient JdbcRowDeserializer rowDeserializer;

  /**
   * Ensure there is only one connection activated.
   */
  private transient Connection connection;
  /**
   * Ensure there is only one statement activated.
   */
  private transient PreparedStatement statement;
  /**
   * Current query result set.
   */
  private transient ResultSet curResultSet;
  /**
   * Split related to current result set.
   */
  private JdbcSourceSplit curSplit;

  public JdbcSourceReader(BitSailConfiguration jobConf, SourceReader.Context readerContext) {
    this.subTaskId = readerContext.getIndexOfSubtask();

    this.dbName = jobConf.getNecessaryOption(JdbcReaderOptions.DB_NAME,
        JdbcErrorCode.REQUIRED_VALUE);
    this.tableName = jobConf.getNecessaryOption(JdbcReaderOptions.TABLE_NAME,
        JdbcErrorCode.REQUIRED_VALUE);
    this.columnInfos = jobConf.getNecessaryOption(JdbcReaderOptions.COLUMNS,
        JdbcErrorCode.REQUIRED_VALUE);
    this.splitField = jobConf.get(JdbcReaderOptions.SPLIT_FIELD);

    // options for select data.
    this.filterSql = jobConf.get(JdbcReaderOptions.SQL_FILTER);
    this.maxFetchCount = jobConf.get(JdbcReaderOptions.MAX_FETCH_COUNT);

    this.splits = new ConcurrentLinkedDeque<>();
    this.connectionHolder = new JdbcConnectionHolder(jobConf);
    this.rowDeserializer = new JdbcRowDeserializer(readerContext.getTypeInfos());
    log.info("Jdbc source reader {} is initialized.", subTaskId);
  }

  @Override
  public void start() {

  }

  @Override
  public void pollNext(SourcePipeline<Row> pipeline) throws Exception {

  }

  @Override
  public void addSplits(List<JdbcSourceSplit> splits) {
    totalSplitNum += splits.size();
    splits.addAll(splits);
  }

  @Override
  public void notifyNoMoreSplits() {
    this.hasNoMoreSplits = true;
    log.info("No more splits will be assigned.");
  }

  @Override
  public boolean hasMoreElements() {
    if (hasNoMoreSplits && splits.isEmpty() && curResultSet == null) {
      log.info("Finish reading all {} splits.", totalSplitNum);
      return false;
    }
    return true;
  }

  @Override
  public List<JdbcSourceSplit> snapshotState(long checkpointId) {
    return Collections.emptyList();
  }

  @Override
  public void close() throws Exception {
    closeQuietly(curResultSet);
    closeQuietly(statement);
    closeQuietly(connectionHolder);

    curResultSet = null;
    statement = null;
    connection = null;
    log.info("Task {} is closed.", subTaskId);
  }

  private void closeQuietly(AutoCloseable closeable) {
    try {
      if (closeable != null) {
        closeable.close();
      }
    } catch (Exception e) {
      log.error("Failed to close {}", closeable.getClass().getSimpleName(), e);
    }
  }
}
