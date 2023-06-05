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

package com.bytedance.bitsail.test.integration.cdc.mysql;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.connector.cdc.model.ClusterInfo;
import com.bytedance.bitsail.connector.cdc.model.ConnectionInfo;
import com.bytedance.bitsail.connector.cdc.option.BinlogReaderOptions;
import com.bytedance.bitsail.connector.kafka.option.KafkaOptions;
import com.bytedance.bitsail.test.integration.AbstractIntegrationTest;
import com.bytedance.bitsail.test.integration.cdc.mysql.container.MySQLContainerMariadbAdapter;
import com.bytedance.bitsail.test.integration.kafka.container.KafkaCluster;
import com.bytedance.bitsail.test.integration.utils.JobConfUtils;

import com.google.common.collect.Lists;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import java.util.stream.Stream;

public class MysqlCDCSourceITCase extends AbstractIntegrationTest {

  private static final Logger LOG = LoggerFactory.getLogger(MysqlCDCSourceITCase.class);
  private static final String MYSQL_DOCKER_IMAGER = "mysql:8.0.29";
  private static final String TEST_USERNAME = "root";
  private static final String TEST_PASSWORD = "pw";
  private static final String TEST_DATABASE = "test";
  private static MySQLContainer<?> container;
  private static final String TOPIC_NAME = "testTopic";
  private static final KafkaCluster KAFKA_CLUSTER = new KafkaCluster();

  @BeforeClass
  public static void before() {
    container = new MySQLContainerMariadbAdapter<>(DockerImageName.parse(MYSQL_DOCKER_IMAGER))
        //.withConfigurationOverride("container/my.cnf")
        .withUrlParam("permitMysqlScheme", null)
        .withInitScript("scripts/jdbc_to_print.sql")
        //.withDatabaseName(TEST_DATABASE)
        .withUsername(TEST_USERNAME)
        .withPassword(TEST_PASSWORD)
        .withLogConsumer(new Slf4jLogConsumer(LOG));
    Startables.deepStart(Stream.of(container)).join();
    KAFKA_CLUSTER.startService();
    KAFKA_CLUSTER.createTopic(TOPIC_NAME);
  }

  @AfterClass
  public static void after() {
    container.close();
    KAFKA_CLUSTER.stopService();
  }

  @Test
  public void testMysqlCDC2Print() throws Exception {
    BitSailConfiguration jobConf = JobConfUtils.fromClasspath("bitsail_mysql_cdc_print.json");
    ConnectionInfo connectionInfo = ConnectionInfo.builder()
        .host(container.getHost())
        .port(container.getFirstMappedPort())
        .url(container.getJdbcUrl())
        .build();
    ClusterInfo clusterInfo = ClusterInfo.builder()
        .master(connectionInfo)
        .build();
    jobConf.set(BinlogReaderOptions.CONNECTIONS, Lists.newArrayList(clusterInfo));
    submitJob(jobConf);
  }

  @Test
  public void testMysqlCDC2Kafka() throws Exception {
    BitSailConfiguration jobConf = JobConfUtils.fromClasspath("bitsail_mysql_cdc_kafka.json");
    ConnectionInfo connectionInfo = ConnectionInfo.builder()
        .host(container.getHost())
        .port(container.getFirstMappedPort())
        .url(container.getJdbcUrl())
        .build();
    ClusterInfo clusterInfo = ClusterInfo.builder()
        .master(connectionInfo)
        .build();
    // set mysql connections info
    jobConf.set(BinlogReaderOptions.CONNECTIONS, Lists.newArrayList(clusterInfo));
    // set kafka config
    jobConf.setWriter(KafkaOptions.BOOTSTRAP_SERVERS, KafkaCluster.getBootstrapServer());
    jobConf.setWriter(KafkaOptions.TOPIC_NAME, TOPIC_NAME);
    submitJob(jobConf);
  }
}
