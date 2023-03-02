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
import com.bytedance.bitsail.connector.kafka.option.KafkaWriterOptions;
import com.bytedance.bitsail.test.integration.AbstractIntegrationTest;
import com.bytedance.bitsail.test.integration.cdc.mysql.container.MySQLContainerMariadbAdapter;
import com.bytedance.bitsail.test.integration.kafka.container.KafkaCluster;
import com.bytedance.bitsail.test.integration.utils.JobConfUtils;

import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import java.util.stream.Stream;

public class MysqlBinlogSourceITCase extends AbstractIntegrationTest {

  private static final Logger LOG = LoggerFactory.getLogger(MysqlBinlogSourceITCase.class);
  private static final String MYSQL_DOCKER_IMAGER = "mysql:8.0.29";
  private static final String TEST_USERNAME = "root";
  private static final String TEST_PASSWORD = "pw";
  private static final String TEST_DATABASE = "test";
  private MySQLContainer<?> container;
  private final String topicName = "testTopic";
  private final KafkaCluster kafkaCluster = new KafkaCluster();

  @Before
  public void before() {
    container = new MySQLContainerMariadbAdapter<>(DockerImageName.parse(MYSQL_DOCKER_IMAGER))
        //.withConfigurationOverride("container/my.cnf")
        .withUrlParam("permitMysqlScheme", null)
        .withInitScript("scripts/jdbc_to_print.sql")
        //.withDatabaseName(TEST_DATABASE)
        .withUsername(TEST_USERNAME)
        .withPassword(TEST_PASSWORD)
        .withLogConsumer(new Slf4jLogConsumer(LOG));
    Startables.deepStart(Stream.of(container)).join();
    kafkaCluster.startService();
    kafkaCluster.createTopic(topicName);
  }

  @After
  public void after() {
    container.close();
    kafkaCluster.stopService();
  }

  //@Test
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

  //@Test
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
    jobConf.set(KafkaWriterOptions.BOOTSTRAP_SERVERS, KafkaCluster.getBootstrapServer());
    jobConf.set(KafkaWriterOptions.TOPIC_NAME, topicName);
    submitJob(jobConf);
  }
}
