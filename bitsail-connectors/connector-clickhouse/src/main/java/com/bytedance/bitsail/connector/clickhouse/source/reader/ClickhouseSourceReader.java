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

package com.bytedance.bitsail.connector.clickhouse.source.reader;

import com.bytedance.bitsail.base.connector.reader.v1.SourcePipeline;
import com.bytedance.bitsail.base.connector.reader.v1.SourceReader;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.model.ColumnInfo;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.connector.clickhouse.error.ClickhouseErrorCode;
import com.bytedance.bitsail.connector.clickhouse.option.ClickhouseReaderOptions;
import com.bytedance.bitsail.connector.clickhouse.source.split.ClickhouseSourceSplit;
import com.bytedance.bitsail.connector.clickhouse.util.ClickhouseConnectionHolder;
import com.bytedance.bitsail.connector.clickhouse.util.ClickhouseJdbcUtils;

import com.clickhouse.jdbc.ClickHouseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;

public class ClickhouseSourceReader implements SourceReader<Row, ClickhouseSourceSplit> {
  private static final Logger LOG = LoggerFactory.getLogger(ClickhouseSourceReader.class);

  private final int subTaskId;
  private int totalSplitNum = 0;
  private boolean hasNoMoreSplits = false;
  private final Deque<ClickhouseSourceSplit> splits;

  private final List<ColumnInfo> columnInfos;
  private final String dbName;
  private final String tableName;
  private final String splitField;

  private final String filterSql;
  private final Long maxFetchCount;

  private final transient ClickhouseConnectionHolder connectionHolder;
  private final transient ClickhouseRowDeserializer rowDeserializer;

  /**
   * Ensure there is only one connection activated.
   */
  private transient ClickHouseConnection connection;
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
  private ClickhouseSourceSplit curSplit;

  public ClickhouseSourceReader(BitSailConfiguration jobConf, SourceReader.Context readerContext) {
    this.subTaskId = readerContext.getIndexOfSubtask();

    this.dbName = jobConf.getNecessaryOption(ClickhouseReaderOptions.DB_NAME,
        ClickhouseErrorCode.REQUIRED_VALUE);
    this.tableName = jobConf.getNecessaryOption(ClickhouseReaderOptions.TABLE_NAME,
        ClickhouseErrorCode.REQUIRED_VALUE);
    this.columnInfos = jobConf.getNecessaryOption(ClickhouseReaderOptions.COLUMNS,
        ClickhouseErrorCode.REQUIRED_VALUE);
    this.splitField = jobConf.get(ClickhouseReaderOptions.SPLIT_FIELD);

    // options for select data.
    this.filterSql = jobConf.get(ClickhouseReaderOptions.SQL_FILTER);
    this.maxFetchCount = jobConf.get(ClickhouseReaderOptions.MAX_FETCH_COUNT);

    this.splits = new ConcurrentLinkedDeque<>();
    this.connectionHolder = new ClickhouseConnectionHolder(jobConf);
    this.rowDeserializer = new ClickhouseRowDeserializer(readerContext.getRowTypeInfo());
    LOG.info("Clickhouse source reader {} is initialized.", subTaskId);
  }

  /**
   * Clickhouse jdbc driver uses streaming query by default.
   * So there is no OOM risk except that a result row is too large to place in memory.<br/>
   * Reference: <a href="https://github.com/ClickHouse/clickhouse-jdbc/issues/929">Clickhouse support streaming query.</a>
   */
  @Override
  public void start() {
    this.connection = connectionHolder.connect();

    // Construct statement.
    String baseSql = ClickhouseJdbcUtils.getQuerySql(dbName, tableName, columnInfos);
    String querySql = ClickhouseJdbcUtils.decorateSql(baseSql, splitField, filterSql, maxFetchCount, true);
    try {
      this.statement = connection.prepareStatement(querySql);
    } catch (SQLException e) {
      throw new RuntimeException("Failed to prepare statement.", e);
    }

    LOG.info("Task {} started.", subTaskId);
  }

  @Override
  public void pollNext(SourcePipeline<Row> pipeline) throws IOException {
    if (curResultSet == null && splits.isEmpty()) {
      return;
    }

    if (curResultSet == null) {
      this.curSplit = splits.poll();
      curSplit.decorateStatement(statement);
      try {
        this.curResultSet = statement.executeQuery();
      } catch (SQLException e) {
        throw new RuntimeException("Failed to get result set.", e);
      }
      LOG.info("Task {} begins to read split: {}=[{}]", subTaskId, curSplit.uniqSplitId(), curSplit);
    }

    boolean hasNext;
    try {
      hasNext = curResultSet.next();
    } catch (SQLException e) {
      throw new IOException("Failed to execute next() for the current result set", e);
    }

    if (hasNext) {
      Row row = rowDeserializer.convert(curResultSet);
      pipeline.output(row);
    } else {
      closeQuietly(curResultSet);
      curResultSet = null;
      LOG.info("Task {} finishes reading rows from split: {}", subTaskId, curSplit.uniqSplitId());
    }
  }

  @Override
  public void addSplits(List<ClickhouseSourceSplit> splitList) {
    totalSplitNum += splitList.size();
    splits.addAll(splitList);
  }

  @Override
  public void notifyNoMoreSplits() {
    this.hasNoMoreSplits = true;
    LOG.info("No more splits will be assigned.");
  }

  @Override
  public boolean hasMoreElements() {
    if (hasNoMoreSplits && splits.isEmpty() && curResultSet == null) {
      LOG.info("Finish reading all {} splits.", totalSplitNum);
      return false;
    }
    return true;
  }

  @Override
  public List<ClickhouseSourceSplit> snapshotState(long checkpointId) {
    return Collections.emptyList();
  }

  @Override
  public void close() {
    closeQuietly(curResultSet);
    closeQuietly(statement);
    closeQuietly(connectionHolder);

    curResultSet = null;
    statement = null;
    connection = null;
    LOG.info("Task {} is closed.", subTaskId);
  }

  private void closeQuietly(AutoCloseable closeable) {
    try {
      if (closeable != null) {
        closeable.close();
      }
    } catch (Exception e) {
      LOG.error("Failed to close {}", closeable.getClass().getSimpleName(), e);
    }
  }
}
