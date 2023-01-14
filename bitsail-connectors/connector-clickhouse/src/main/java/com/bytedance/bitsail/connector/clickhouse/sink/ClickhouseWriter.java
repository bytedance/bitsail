package com.bytedance.bitsail.connector.clickhouse.sink;

import com.bytedance.bitsail.base.connector.writer.v1.Writer;
import com.bytedance.bitsail.base.connector.writer.v1.state.EmptyState;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.model.ColumnInfo;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.connector.clickhouse.error.ClickhouseErrorCode;
import com.bytedance.bitsail.connector.clickhouse.option.ClickhouseWriterOptions;
import com.bytedance.bitsail.connector.clickhouse.source.reader.ClickhouseSourceReader;
import com.bytedance.bitsail.connector.clickhouse.util.ClickhouseConnectionHolder;

import com.clickhouse.jdbc.ClickHouseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.domain.ClickHouseDataType;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

import static com.bytedance.bitsail.connector.clickhouse.constant.ClickhouseConstants.CLICKHOUSE_DECIMAL_INPUT_TYPE;

public class ClickhouseWriter<CommitT> implements Writer<Row, CommitT, EmptyState> {
  private static final Logger LOG = LoggerFactory.getLogger(ClickhouseSourceReader.class);

  private final int subTaskId;

  private final String jdbcUrl;
  private final List<ColumnInfo> columnInfos;
  private final String dbName;
  private final String tableName;

  private final String userName;
  private final String password;
  /*private final String writeMode;*/
  /*private final String writerParallelismNum;*/
  private final Integer batchSize;

  private final String insertSql;

  private final transient ClickhouseConnectionHolder connectionHolder;

  private final List<Row> writeBuffer;
  /*private final List<Row> commitBuffer;*/
  private final AtomicInteger printCount;

  /**
   * Ensure there is only one connection activated.
   */
  private transient ClickHouseConnection connection;
  /**
   * Ensure there is only one statement activated.
   */
  private transient PreparedStatement statement;

  public ClickhouseWriter(BitSailConfiguration jobConf, Writer.Context<EmptyState> context) {
    this.subTaskId = context.getIndexOfSubTaskId();

    this.jdbcUrl = jobConf.getNecessaryOption(ClickhouseWriterOptions.JDBC_URL,
      ClickhouseErrorCode.REQUIRED_VALUE);
    this.userName = jobConf.getNecessaryOption(ClickhouseWriterOptions.USER_NAME,
      ClickhouseErrorCode.REQUIRED_VALUE);
    this.password = jobConf.getNecessaryOption(ClickhouseWriterOptions.PASSWORD,
      ClickhouseErrorCode.REQUIRED_VALUE);

    this.dbName = jobConf.getNecessaryOption(ClickhouseWriterOptions.DB_NAME,
      ClickhouseErrorCode.REQUIRED_VALUE);
    this.tableName = jobConf.getNecessaryOption(ClickhouseWriterOptions.TABLE_NAME,
      ClickhouseErrorCode.REQUIRED_VALUE);

    /*this.writeMode = jobConf.get(ClickhouseWriterOptions.TABLE_NAME);*/
    /*this.writerParallelismNum = jobConf.get(ClickhouseWriterOptions.WRITER_PARALLELISM_NUM);*/
    this.batchSize = jobConf.get(ClickhouseWriterOptions.BATCH_SIZE);

    this.columnInfos = jobConf.getNecessaryOption(ClickhouseWriterOptions.COLUMNS,
      ClickhouseErrorCode.REQUIRED_VALUE);

    connectionHolder = new ClickhouseConnectionHolder(jobConf, true);
    this.connection = connectionHolder.connect();

    insertSql = this.insertSql();
    try {
      this.statement = connection.prepareStatement(insertSql);
    } catch (SQLException e) {
      throw new RuntimeException("Failed to prepare statement.", e);
    }

    this.writeBuffer = new ArrayList<>(batchSize);
    /*this.commitBuffer = new ArrayList<>(batchSize);*/
    this.printCount = new AtomicInteger(0);

    LOG.info("Clickhouse sink writer {} is initialized.", subTaskId);
  }

  @Override
  public void write(Row element) throws IOException {
    this.writeBuffer.add(element);

    if (writeBuffer.size() == batchSize) {
      this.flush(false);
    }
    printCount.incrementAndGet();
  }

  @Override
  public void flush(boolean endOfInput) throws IOException {
    /*this.commitBuffer.addAll(this.writeBuffer);*/

    try {
      for (Row ele : writeBuffer) {
        for (int i = 0; i < this.columnInfos.size(); i++) {
          statement.setObject(i + 1, ele.getField(i));
        }
        statement.addBatch();
      }
      statement.executeBatch();
    } catch (SQLException e) {
      throw new RuntimeException("Failed to write batch.", e);
    }

    writeBuffer.clear();
    if (endOfInput) {
      LOG.info("all records are sent to commit buffer.");
    }
  }

  @Override
  public List<CommitT> prepareCommit() throws IOException {
    return Collections.emptyList();
  }

  @Override
  public void close() throws IOException {
    Writer.super.close();

    try {
      this.statement.close();
      this.connection.close();
    } catch (SQLException e) {
      throw new RuntimeException("Failed to close clickhouse writer.", e);
    }
  }

  private String insertSql() {
    String inputFields = this.columnInfos.stream().map(col -> {
      if (Objects.isNull(col.getNullable()) || !col.getNullable()) {
        return String.format("%s %s", col.getName(), this.getClickhouseType(col.getType()));
      } else {
        return String.format("%s Nullable(%s)", col.getName(), this.getClickhouseType(col.getType()));
      }
    }).reduce((n1, n2) -> String.format("%s, %s", n1, n2)).get();

    StringBuffer sql = new StringBuffer("INSERT INTO ");
    sql.append(String.format("%s.%s", this.dbName, this.tableName))
        .append(" SELECT ")
        .append(this.columnInfos.stream().map(col -> col.getName()).reduce((n1, n2) -> String.format("%s, %s", n1, n2)).get())
        .append(" FROM input('")
        .append(inputFields)
        .append("')");

    return sql.toString();
  }

  private String getClickhouseType(String inputType) {
    ClickHouseDataType ckType = ClickHouseDataType.fromTypeString(inputType);
    String strType = ckType.toString();

    if (strType.toLowerCase().contains("decimal")) {
      return CLICKHOUSE_DECIMAL_INPUT_TYPE;
    }
    return strType;
  }

}