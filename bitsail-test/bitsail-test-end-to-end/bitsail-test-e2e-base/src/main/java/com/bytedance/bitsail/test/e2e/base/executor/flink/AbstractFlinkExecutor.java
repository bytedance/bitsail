/*
 * Copyright 2022 Bytedance Ltd. and/or its affiliates.
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

package com.bytedance.bitsail.test.e2e.base.executor.flink;

import com.bytedance.bitsail.base.packages.PluginFinder;
import com.bytedance.bitsail.base.packages.PluginFinderFactory;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.option.ReaderOptions;
import com.bytedance.bitsail.common.option.WriterOptions;
import com.bytedance.bitsail.test.e2e.base.TestOptions;
import com.bytedance.bitsail.test.e2e.base.executor.AbstractExecutor;
import com.bytedance.bitsail.test.e2e.base.transfer.FileMappingTransfer;
import com.bytedance.bitsail.test.e2e.base.transfer.TransferableFile;

import com.google.common.collect.Lists;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerLoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.stream.Stream;

public abstract class AbstractFlinkExecutor extends AbstractExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractFlinkExecutor.class);

  /**
   * Plugin finder to locate libs.
   */
  @Setter
  protected static PluginFinder pluginFinder = PluginFinderFactory.getPluginFinder("localFS");

  /**
   * Whole test configuration.
   */
  protected BitSailConfiguration conf;

  /**
   * Flink dir in test container.
   */
  protected String flinkRootDir;

  /**
   * Flink docker.
   */
  protected GenericContainer<?> executor;

  /**
   * Timeout of test in seconds.
   */
  protected int executeTimeout;

  @Override
  public void configure(BitSailConfiguration executorConf) {
    super.configure(executorConf);
    this.conf = executorConf;
    this.flinkRootDir = getFlinkRootDir();
    this.executeTimeout = executorConf.get(TestOptions.TEST_TIMEOUT);

    registerEngine();
    registerConnector(conf.get(ReaderOptions.READER_CLASS));
    registerConnector(conf.get(WriterOptions.WRITER_CLASS));
    registerJobConf();
  }

  /**
   * Register execute script, global bitsail conf, engine libs, and clients libs.
   */
  protected void registerEngine() {
    String sysConfPath;
    try {
      sysConfPath = Paths.get(getClass().getResource("/conf/bitsail.conf").toURI()).toString();
    } catch (Exception e) {
      throw new RuntimeException("Failed to load sys conf for test.");
    }
    transferableFiles.add(new TransferableFile(sysConfPath,
        Paths.get(executorRootDir, "conf/bitsail.conf").toAbsolutePath().toString()));

    List<String> engineFiles = Lists.newArrayList(
        "bin/",
        "conf/logback.xml",
        "libs/bitsail-core.jar",
        "libs/clients/",
        "libs/engines/bitsail-engine-flink-" + revision + ".jar",
        "libs/engines/mapping/",
        "libs/connectors/mapping/"
    );

    FileMappingTransfer transfer = new FileMappingTransfer(localRootDir, executorRootDir);
    transferableFiles.addAll(transfer.transfer(engineFiles));
  }

  /**
   * Register connectors.
   */
  protected void registerConnector(String connector) {
    List<String> connectorFiles = Lists.newArrayList();
    pluginFinder.loadPlugin(connector).stream()
        .map(lib -> "libs/connectors/" + Paths.get(lib.getFile()).getFileName())
        .forEach(connectorFiles::add);
    FileMappingTransfer transfer = new FileMappingTransfer(localRootDir, executorRootDir);
    transferableFiles.addAll(transfer.transfer(connectorFiles));
  }

  /**
   * Register job configuration file.
   */
  protected void registerJobConf() {
    File tmpJobConf;
    try {
      tmpJobConf = File.createTempFile("jobConf", ".json");
      tmpJobConf.deleteOnExit();
      BufferedWriter out = new BufferedWriter(new FileWriter(tmpJobConf));
      out.write(conf.toJSON());
      out.newLine();
      out.close();
    } catch (IOException e) {
      throw new RuntimeException("Failed to create tmp job conf file.");
    }

    transferableFiles.add(new TransferableFile(tmpJobConf.getAbsolutePath(),
        Paths.get(executorRootDir, "jobConf.json").toAbsolutePath().toString()));
  }

  @Override
  protected void initNetwork() {
    network = Network.newNetwork();
  }

  @Override
  public void init() {
    initNetwork();
    String flinkDockerImage = getFlinkDockerImage();

    executor = new GenericContainer<>(flinkDockerImage)
        .withNetwork(network)
        .withLogConsumer(new Slf4jLogConsumer(DockerLoggerFactory.getLogger(flinkDockerImage)).withSeparateOutputStreams())
        .withStartupAttempts(1)
        .withWorkingDirectory(executorRootDir)
        .waitingFor(new LogMessageWaitStrategy()
        .withRegEx(".*BitSail Job Finished.*")
        .withStartupTimeout(Duration.ofSeconds(executeTimeout)));

    for (TransferableFile file : transferableFiles) {
      copyToContainer(executor, file);
    }
  }

  public int run() {
    executor.withCommand("bash", "-c", String.join(" ", getExecCommand()));
    Startables.deepStart(Stream.of(executor)).join();
    return 0;
  }

  @Override
  public void close() throws IOException {
    if (executor != null) {
      executor.stop();
      executor.close();
      executor = null;
    }
    super.close();
  }

  /**
   * Opensource flink docker image.
   */
  protected abstract String getFlinkDockerImage();

  /**
   * Initialize the root dir of flink in test docker.
   */
  protected abstract String getFlinkRootDir();

  /**
   * Commands for running bitsail e2e test.
   */
  protected abstract List<String> getExecCommand();
}
