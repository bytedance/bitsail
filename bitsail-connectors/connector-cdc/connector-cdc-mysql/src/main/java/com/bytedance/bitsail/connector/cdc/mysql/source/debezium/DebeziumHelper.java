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

package com.bytedance.bitsail.connector.cdc.mysql.source.debezium;

import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.util.Pair;
import com.bytedance.bitsail.connector.cdc.error.BinlogReaderErrorCode;
import com.bytedance.bitsail.connector.cdc.mysql.source.constant.MysqlConstant;
import com.bytedance.bitsail.connector.cdc.source.offset.BinlogOffset;
import com.bytedance.bitsail.connector.cdc.source.split.BinlogSplit;

import io.debezium.DebeziumException;
import io.debezium.connector.mysql.MySqlConnection;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.MySqlDatabaseSchema;
import io.debezium.connector.mysql.MySqlOffsetContext;
import io.debezium.connector.mysql.MySqlTopicSelector;
import io.debezium.connector.mysql.MySqlValueConverters;
import io.debezium.connector.mysql.SourceInfo;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;
import io.debezium.util.SchemaNameAdjuster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Map;

public class DebeziumHelper {

  private static final Logger LOG = LoggerFactory.getLogger(DebeziumHelper.class);

  public static MySqlValueConverters getValueConverters(MySqlConnectorConfig configuration) {
    // Use MySQL-specific converters and schemas for values ...

    TemporalPrecisionMode timePrecisionMode = configuration.getTemporalPrecisionMode();

    JdbcValueConverters.DecimalMode decimalMode = configuration.getDecimalMode();

    String bigIntUnsignedHandlingModeStr = configuration.getConfig().getString(MySqlConnectorConfig.BIGINT_UNSIGNED_HANDLING_MODE);
    MySqlConnectorConfig.BigIntUnsignedHandlingMode bigIntUnsignedHandlingMode = MySqlConnectorConfig.BigIntUnsignedHandlingMode.parse(bigIntUnsignedHandlingModeStr);
    JdbcValueConverters.BigIntUnsignedMode bigIntUnsignedMode = bigIntUnsignedHandlingMode.asBigIntUnsignedMode();

    final boolean timeAdjusterEnabled = configuration.getConfig().getBoolean(MySqlConnectorConfig.ENABLE_TIME_ADJUSTER);
    return new MySqlValueConverters(decimalMode, timePrecisionMode, bigIntUnsignedMode,
        configuration.binaryHandlingMode(), timeAdjusterEnabled ? MySqlValueConverters::adjustTemporal : x -> x,
        MySqlValueConverters::defaultParsingErrorHandler);
  }

  public static void validateBinlogConfiguration(MySqlConnectorConfig config, MySqlConnection connection) {
    if (config.getSnapshotMode().shouldStream()) {
      // Check whether the row-level binlog is enabled ...
      final boolean binlogFormatRow = isBinlogFormatRow(connection);
      final boolean binlogRowImageFull = isBinlogRowImageFull(connection);
      final boolean rowBinlogEnabled = binlogFormatRow && binlogRowImageFull;

      if (!rowBinlogEnabled) {
        if (!binlogFormatRow) {
          throw new DebeziumException("The MySQL server is not configured to use a ROW binlog_format, which is "
              + "required for this connector to work properly. Change the MySQL configuration to use a "
              + "binlog_format=ROW and restart the connector.");
        } else {
          throw new DebeziumException("The MySQL server is not configured to use a FULL binlog_row_image, which is "
              + "required for this connector to work properly. Change the MySQL configuration to use a "
              + "binlog_row_image=FULL and restart the connector.");
        }
      }
    }
  }

  public static boolean isBinlogFormatRow(MySqlConnection connection) {
    try {
      final String mode = connection.queryAndMap("SHOW GLOBAL VARIABLES LIKE 'binlog_format'", rs -> rs.next() ? rs.getString(2) : "");
      LOG.info("binlog_format={}", mode);
      return "ROW".equalsIgnoreCase(mode);
    } catch (SQLException e) {
      throw new DebeziumException("Unexpected error while connecting to MySQL and looking at BINLOG_FORMAT mode: ", e);
    }
  }

  public static boolean isBinlogRowImageFull(MySqlConnection connection) {
    try {
      final String rowImage = connection.queryAndMap("SHOW GLOBAL VARIABLES LIKE 'binlog_row_image'", rs -> {
        if (rs.next()) {
          return rs.getString(2);
        }
        // This setting was introduced in MySQL 5.6+ with default of 'FULL'.
        // For older versions, assume 'FULL'.
        return "FULL";
      });
      LOG.info("binlog_row_image={}", rowImage);
      return "FULL".equalsIgnoreCase(rowImage);
    } catch (SQLException e) {
      throw new DebeziumException("Unexpected error while connecting to MySQL and looking at BINLOG_ROW_IMAGE mode: ", e);
    }
  }

  public static BinlogOffset convertDbzOffsetToBinlogOffset(Map<String, String> dbzOffset) {
    String fileName = dbzOffset.get("file");
    String position = dbzOffset.get("pos");
    BinlogOffset offset = BinlogOffset.specified();
    offset.addProps(MysqlConstant.BINLOG_PROPS_FILENAME, fileName);
    offset.addProps(MysqlConstant.BINLOG_PROPS_OFFSET, position);
    return offset;
  }

  public static MySqlOffsetContext loadOffsetContext(MySqlConnectorConfig config, BinlogSplit split, MySqlConnection connection) {
    final MySqlOffsetContext offsetContext = new MySqlOffsetContext(config, false, false, new SourceInfo(config));
    switch (split.getBeginOffset().getOffsetType()) {
      case EARLIEST:
        offsetContext.setBinlogStartPoint("", 0L);
        break;
      case LATEST:
        Pair<String, Long> latestOffset = getLatestOffset(connection);
        offsetContext.setBinlogStartPoint(latestOffset.getFirst(), latestOffset.getSecond());
        break;
      case SPECIFIED:
        BinlogOffset offset = split.getBeginOffset();
        String binlogFilename = offset.getProps().get(MysqlConstant.BINLOG_PROPS_FILENAME);
        String binlogOffset = offset.getProps().get(MysqlConstant.BINLOG_PROPS_OFFSET);
        offsetContext.setBinlogStartPoint(binlogFilename, Long.parseLong(binlogOffset));
        break;
      default:
        throw new BitSailException(BinlogReaderErrorCode.UNSUPPORTED_ERROR,
            String.format("the begin binlog type %s is not supported", split.getBeginOffset().getOffsetType()));
    }
    return offsetContext;
  }

  /**
   * Get the latest offset of mysql.
   */
  public static Pair<String, Long> getLatestOffset(MySqlConnection connection) {
    try {
      return connection.queryAndMap("SHOW MASTER STATUS", rs -> {
        if (rs.next()) {
          String binlogFilename = rs.getString(1);
          long binlogOffset = rs.getLong(2);
          return new Pair<>(binlogFilename, binlogOffset);
        } else {
          throw new BitSailException(BinlogReaderErrorCode.OFFSET_ERROR,
              "Load latest offset failed, please check your mysql connection is working");
        }
      });
    } catch (SQLException e) {
      throw new BitSailException(BinlogReaderErrorCode.OFFSET_ERROR,
          "Load latest offset failed, please check your mysql connection is working");
    }
  }

  public static MySqlDatabaseSchema createMySqlDatabaseSchema(
      MySqlConnectorConfig dbzMySqlConfig, boolean isTableIdCaseSensitive) {
    TopicSelector<TableId> topicSelector = MySqlTopicSelector.defaultSelector(dbzMySqlConfig);
    SchemaNameAdjuster schemaNameAdjuster = SchemaNameAdjuster.create();
    MySqlValueConverters valueConverters = getValueConverters(dbzMySqlConfig);
    return new MySqlDatabaseSchema(
        dbzMySqlConfig,
        valueConverters,
        topicSelector,
        schemaNameAdjuster,
        isTableIdCaseSensitive);
  }
}
