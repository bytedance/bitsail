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

package com.bytedance.bitsail.connector.cdc.mysql.source.config;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.connector.cdc.model.ClusterInfo;
import com.bytedance.bitsail.connector.cdc.model.ConnectionInfo;
import com.bytedance.bitsail.connector.cdc.option.BinlogReaderOptions;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class MysqlConfigTest {

  private static String HOST_NAME = "192.168";
  private static String URL = "123";
  private static int PORT = 123;
  private static String USERNAME = "gary";
  private static String PASSWORD = "666";

  @Test
  public void testMysqlConfigConversion() {
    BitSailConfiguration jobConf = BitSailConfiguration.newDefault();
    List<ClusterInfo> cluster = new ArrayList<>();
    ClusterInfo instant = new ClusterInfo();
    instant.setMaster(new ConnectionInfo(HOST_NAME, URL, PORT));
    cluster.add(instant);
    jobConf.set(BinlogReaderOptions.USER_NAME, USERNAME);
    jobConf.set(BinlogReaderOptions.PASSWORD, PASSWORD);
    jobConf.set(BinlogReaderOptions.CONNECTIONS, cluster);

    //add debezium properties
    jobConf.set(MysqlConfig.DEBEZIUM_PREFIX + "key1", "value1");
    jobConf.set(MysqlConfig.DEBEZIUM_PREFIX + "key2", "value2");

    MysqlConfig mysqlConfig = MysqlConfig.fromBitSailConf(jobConf, 1L);
    Assert.assertEquals(HOST_NAME, mysqlConfig.getHostname());
    Assert.assertEquals(PORT, mysqlConfig.getPort());
    Assert.assertEquals(USERNAME, mysqlConfig.getUsername());
    Assert.assertEquals(PASSWORD, mysqlConfig.getPassword());
    Assert.assertEquals("value1", mysqlConfig.getDbzProperties().getProperty("key1"));
    Assert.assertEquals("value2", mysqlConfig.getDbzProperties().getProperty("key2"));
  }
}
