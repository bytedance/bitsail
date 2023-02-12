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
package com.bytedance.bitsail.connector.cdc.postgres.source.config;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.connector.cdc.model.ClusterInfo;
import com.bytedance.bitsail.connector.cdc.model.ConnectionInfo;
import com.bytedance.bitsail.connector.cdc.option.BinlogReaderOptions;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class PostgresConfigTest {
  private static String HOST_NAME = "192.168";
  private static String URL = "123";
  private static int PORT = 123;
  private static String USERNAME = "gary";
  private static String PASSWORD = "666";
  private static String DATABASE = "test_cdc";

  @Test
  public void testMysqlConfigConversion() {
    BitSailConfiguration jobConf = BitSailConfiguration.newDefault();
    List<ClusterInfo> cluster = new ArrayList<>();
    ClusterInfo instant = new ClusterInfo();
    instant.setMaster(new ConnectionInfo(HOST_NAME, URL, PORT));
    cluster.add(instant);
    jobConf.set(BinlogReaderOptions.USER_NAME, USERNAME);
    jobConf.set(BinlogReaderOptions.PASSWORD, PASSWORD);
    jobConf.set(BinlogReaderOptions.DB_NAME, DATABASE);
    jobConf.set(BinlogReaderOptions.CONNECTIONS, cluster);

    //add debezium properties
    jobConf.set(PostgresConfig.DEBEZIUM_PREFIX + "key1", "value1");
    jobConf.set(PostgresConfig.DEBEZIUM_PREFIX + "key2", "value2");

    PostgresConfig postgresConfig = new PostgresConfig(jobConf);
    Assert.assertEquals(HOST_NAME, postgresConfig.getHostname());
    Assert.assertEquals(PORT, postgresConfig.getPort());
    Assert.assertEquals(USERNAME, postgresConfig.getUsername());
    Assert.assertEquals(DATABASE, postgresConfig.getDbName());
    Assert.assertEquals(PASSWORD, postgresConfig.getPassword());
    Assert.assertEquals("value1", postgresConfig.getDbzProperties().getProperty("key1"));
    Assert.assertEquals("value2", postgresConfig.getDbzProperties().getProperty("key2"));
  }
}

