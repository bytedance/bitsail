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

package com.bytedance.bitsail.connector.legacy.jdbc.sink;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.connector.legacy.jdbc.container.MySQLContainerMariadbAdapter;
import com.bytedance.bitsail.connector.legacy.jdbc.model.ConnectionInfo;
import com.bytedance.bitsail.connector.legacy.jdbc.options.JdbcWriterOptions;
import com.bytedance.bitsail.test.connector.test.EmbeddedFlinkCluster;
import com.bytedance.bitsail.test.connector.test.utils.JobConfUtils;

import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import java.util.stream.Stream;

/**
 * Created 2022/7/26
 */
public class JdbcSinkITCase {
  private static final Logger LOG = LoggerFactory.getLogger(JdbcSinkITCase.class);

  private static final String MYSQL_DOCKER_IMAGER = "mysql:8.0.29";

  private MySQLContainer<?> container;

  @Before
  public void before() {
    container = new MySQLContainerMariadbAdapter<>(DockerImageName.parse(MYSQL_DOCKER_IMAGER))
        .withUrlParam("permitMysqlScheme", null)
        .withInitScript("scripts/fake_to_jdbc_sink.sql")
        .withLogConsumer(new Slf4jLogConsumer(LOG));

    Startables.deepStart(Stream.of(container)).join();
  }

  @After
  public void after() {
    container.close();
  }

  @Test
  public void testInsertModeMySQL() throws Exception {
    BitSailConfiguration globalConfiguration = JobConfUtils.fromClasspath("scripts/fake_to_jdbc_sink.json");

    ConnectionInfo connectionInfo = new ConnectionInfo();
    connectionInfo.setHost(container.getHost());
    connectionInfo.setPort(container.getFirstMappedPort());
    connectionInfo.setUrl(container.getJdbcUrl());
    globalConfiguration.set(JdbcWriterOptions.CONNECTIONS, Lists.newArrayList(connectionInfo));
    EmbeddedFlinkCluster.submitJob(globalConfiguration);
  }
}