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

import com.bytedance.bitsail.connector.cdc.mysql.source.config.MysqlConfig;
import com.bytedance.bitsail.connector.cdc.mysql.source.constant.MysqlConstant;
import com.bytedance.bitsail.connector.cdc.source.offset.BinlogOffset;
import com.bytedance.bitsail.connector.cdc.source.split.BinlogSplit;

import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.MySqlOffsetContext;
import org.junit.Assert;
import org.junit.Test;

public class DebeziumHelperTest {
  @Test
  public void testLoadEarliestOffsetContext() {
    MysqlConfig mysqlConfig = MysqlConfig.newDefault();
    MySqlConnectorConfig connectorConfig = mysqlConfig.getDbzMySqlConnectorConfig();
    BinlogSplit split = new BinlogSplit("split-0",
        BinlogOffset.earliest(),
        BinlogOffset.boundless());
    MySqlOffsetContext offsetContext = DebeziumHelper.loadOffsetContext(connectorConfig, split, null);
    Assert.assertEquals("", offsetContext.getSource().binlogFilename());
    Assert.assertEquals(0L, offsetContext.getSource().binlogPosition());
  }

  @Test
  public void testLoadSpecifiedOffsetContext() {
    String filename = "mysql.001";
    String binlogOffset = "111";
    MysqlConfig mysqlConfig = MysqlConfig.newDefault();
    MySqlConnectorConfig connectorConfig = mysqlConfig.getDbzMySqlConnectorConfig();
    BinlogOffset offset = BinlogOffset.specified();
    offset.addProps(MysqlConstant.BINLOG_PROPS_FILENAME, filename);
    offset.addProps(MysqlConstant.BINLOG_PROPS_OFFSET, binlogOffset);

    BinlogSplit split = new BinlogSplit("split-0",
        offset,
        BinlogOffset.boundless());
    MySqlOffsetContext offsetContext = DebeziumHelper.loadOffsetContext(connectorConfig, split, null);
    Assert.assertEquals(filename, offsetContext.getSource().binlogFilename());
    Assert.assertEquals(Long.parseLong(binlogOffset), offsetContext.getSource().binlogPosition());
  }
}
