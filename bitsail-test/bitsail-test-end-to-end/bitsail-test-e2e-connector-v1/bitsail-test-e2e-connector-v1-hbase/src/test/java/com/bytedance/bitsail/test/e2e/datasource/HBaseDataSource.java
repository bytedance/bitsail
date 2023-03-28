/*
 *       Copyright 2022-2023 Bytedance Ltd. and/or its affiliates.
 *
 *       Licensed under the Apache License, Version 2.0 (the "License");
 *       you may not use this file except in compliance with the License.
 *       You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *       Unless required by applicable law or agreed to in writing, software
 *       distributed under the License is distributed on an "AS IS" BASIS,
 *       WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *       See the License for the specific language governing permissions and
 *       limitations under the License.
 */

package com.bytedance.bitsail.test.e2e.datasource;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;

import lombok.SneakyThrows;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class HBaseDataSource extends AbstractDataSource {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseDataSource.class);

  private static final DockerImageName HBASE_DOCKER_IMAGE = DockerImageName
      .parse("jcjabouille/hbase-standalone:2.4.9");

  private Network network;
  private HbaseContainer hbaseContainer;
  private Connection connection;

  @Override
  public String getContainerName() {
    return "hbase-container";
  }

  @Override
  public void initNetwork(Network executorNetwork) {
    this.network = executorNetwork;
  }

  @Override
  public boolean accept(BitSailConfiguration jobConf, Role role) {
    return true;
  }

  @Override
  public void configure(BitSailConfiguration dataSourceConf) {

  }

  @Override
  public void modifyJobConf(BitSailConfiguration jobConf) {

  }

  @Override
  public void start() {
    try {
      hbaseContainer = new HbaseContainer(HBASE_DOCKER_IMAGE);
      hbaseContainer.start();
      hbaseContainer.waitingFor(Wait.defaultWaitStrategy());
      Configuration configuration = hbaseContainer.getConfiguration();
      connection = ConnectionFactory.createConnection(configuration);
      LOG.info("Successfully start hbase service, zookeeper quorum: {}", hbaseContainer.getZookeeperQuorum());

      createTable("test_table", Arrays.asList("cf1", "cf2", "cf3"));
      putRow("test_table", "row1", "cf1", "int1", "10");
      putRow("test_table", "row1", "cf1", "str1", "aaaaa");
      putRow("test_table", "row1", "cf2", "int2", "0");
      putRow("test_table", "row1", "cf3", "str2", "ccccc");
      putRow("test_table", "row2", "cf1", "int1", "10");
      putRow("test_table", "row2", "cf1", "str1", "bbbbb");
      putRow("test_table", "row2", "cf2", "int2", "0");
      putRow("test_table", "row2", "cf3", "str2", "ddddd");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void createTable(String tableName, List<String> list) throws IOException {

    TableDescriptorBuilder tableDesc = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));

    List<ColumnFamilyDescriptor> colFamilyList = new ArrayList<>();
    for (String columnFamilys : list) {
      ColumnFamilyDescriptorBuilder c = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(columnFamilys));
      colFamilyList.add(c.build());
    }
    tableDesc.setColumnFamilies(colFamilyList);
    Admin hbaseAdmin = connection.getAdmin();
    hbaseAdmin.createTable(tableDesc.build());
  }

  @SneakyThrows
  public void putRow(String tableName, String rowKey, String columnFamilyName, String qualifier,
                     String value) {
    Table table = connection.getTable(TableName.valueOf(tableName));
    Put put = new Put(Bytes.toBytes(rowKey));
    put.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes(qualifier), Bytes.toBytes(value));
    table.put(put);
    table.close();
  }

  @Override
  public void close() throws IOException {
    if (Objects.nonNull(connection)) {
      connection.close();
    }
    if (Objects.nonNull(hbaseContainer)) {
      hbaseContainer.close();
    }
    hbaseContainer = null;
  }
}
