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

package com.bytedance.bitsail.test.integration.legacy.hbase.container;

import lombok.Getter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.Arrays;

public class HbaseContainer extends GenericContainer<HbaseContainer> {

  private static final int ZOOKEEPER_PORT = 2181;
  private static final int MASTER_PORT = 16000;
  private static final int REGION_PORT = 16020;
  private static final String DEFAULT_HOST = "localhost";
  private static final int STARTUP_TIMEOUT = 5;
  @Getter
  private Configuration configuration;

  public HbaseContainer(DockerImageName dockerImageName) throws UnknownHostException {
    super(dockerImageName);
    configuration = HBaseConfiguration.create();
    addExposedPort(MASTER_PORT);
    addExposedPort(REGION_PORT);
    addExposedPort(ZOOKEEPER_PORT);
    String hostname = InetAddress.getLocalHost().getHostName();

    withCreateContainerCmdModifier(cmd -> cmd.withHostName(hostname));
    withLogConsumer(frame -> System.out.print(frame.getUtf8String()));
    waitingFor(Wait.forLogMessage(".*0 row.*", 1));
    withStartupTimeout(Duration.ofMinutes(STARTUP_TIMEOUT));
    withEnv("HBASE_MASTER_PORT", String.valueOf(MASTER_PORT));
    withEnv("HBASE_REGION_PORT", String.valueOf(REGION_PORT));
    withEnv("HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT", String.valueOf(ZOOKEEPER_PORT));
    withEnv("HBASE_ZOOKEEPER_QUORUM", DEFAULT_HOST);

    setPortBindings(Arrays.asList(String.format("%s:%s", MASTER_PORT, MASTER_PORT), String.format("%s:%s", REGION_PORT, REGION_PORT),
        String.format("%s:%s", ZOOKEEPER_PORT, ZOOKEEPER_PORT)));

  }

  @Override
  protected void doStart() {
    super.doStart();
    configuration.set("hbase.client.pause", "200");
    configuration.set("hbase.client.retries.number", "10");
    configuration.set("hbase.rpc.timeout", "3000");
    configuration.set("hbase.client.operation.timeout", "3000");
    configuration.set("hbase.client.scanner.timeout.period", "10000");
    configuration.set("zookeeper.session.timeout", "10000");
    configuration.set("hbase.zookeeper.quorum", DEFAULT_HOST);
    configuration.set("hbase.zookeeper.property.clientPort", getMappedPort(ZOOKEEPER_PORT).toString());
    configuration.set("hbase.master.port", getMappedPort(MASTER_PORT).toString());
    configuration.set("hbase.regionserver.port", getMappedPort(REGION_PORT).toString());
  }

  public String getZookeeperQuorum() {
    return String.format("%s:%s", getHost(), getMappedPort(ZOOKEEPER_PORT));
  }
}
