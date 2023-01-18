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

package com.bytedance.bitsail.test.e2e.executor.flink;

import com.bytedance.bitsail.base.packages.PluginFinder;
import com.bytedance.bitsail.base.packages.PluginFinderFactory;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.option.CommonOptions;
import com.bytedance.bitsail.common.option.ReaderOptions;
import com.bytedance.bitsail.common.option.WriterOptions;
import com.bytedance.bitsail.test.e2e.base.transfer.FileMappingTransfer;
import com.bytedance.bitsail.test.e2e.base.transfer.TransferableFile;
import com.bytedance.bitsail.test.e2e.executor.AbstractExecutor;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.stream.Stream;

public abstract class AbstractFlinkExecutor extends AbstractExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractFlinkExecutor.class);

  protected static final String EXECUTOR_READY_MSG = "BitSail Test Executor Ready.";

  /**
   * Plugin finder to locate libs.
   */
  protected PluginFinder pluginFinder;

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

  @Override
  public void configure(BitSailConfiguration executorConf) {
    super.configure(executorConf);
    this.conf = executorConf;
    this.flinkRootDir = getFlinkRootDir();

    setPluginFinder(PluginFinderFactory.getPluginFinder("localFS"));
    registerEngine();
    registerConnector(conf.get(ReaderOptions.READER_CLASS));
    registerConnector(conf.get(WriterOptions.WRITER_CLASS));
    registerJobConf();
  }

  public void setPluginFinder(PluginFinder pluginFinder) {
    if (this.pluginFinder == null) {
      this.pluginFinder = pluginFinder;
    }
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
    BitSailConfiguration jobConf = conf.clone();
    Path libPath = Paths.get(executorRootDir, "libs").toAbsolutePath();
    jobConf.set(CommonOptions.JOB_PLUGIN_ROOT_PATH, libPath.toString());

    File tmpJobConf;
    try {
      tmpJobConf = File.createTempFile("jobConf", ".json");
      tmpJobConf.deleteOnExit();
      BufferedWriter out = new BufferedWriter(new FileWriter(tmpJobConf));
      out.write(jobConf.toJSON());
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
        .withCommand("bash", "-c", "echo " + EXECUTOR_READY_MSG + "; while true; do sleep 5; done")
        .waitingFor(new LogMessageWaitStrategy()
        .withRegEx(".*" + EXECUTOR_READY_MSG + ".*")
        .withStartupTimeout(Duration.ofSeconds(30)));

    for (TransferableFile file : transferableFiles) {
      copyToContainer(executor, file);
    }
  }

  public int run(String testId) throws IOException, InterruptedException {
    Startables.deepStart(Stream.of(executor)).join();

    String commands = String.join(" ", getExecCommand());
    LOG.info("Begin test: [{}], Container: [{}]\n"
        + "=============== Test Command ===============\n"
        + commands + "\n"
        + "============================================\n",
        testId, getContainerName());

    Container.ExecResult result = executor.execInContainer("bash", "-c", commands);

    String stdOut = result.getStdout();
    String stdErr = result.getStderr();
    int exitCode = result.getExitCode();

    LOG.info("Finish test : [{}], Container: [{}]\n"
        + "================== STDOUT ==================\n"
        + "{}\n"
        + "================== STDERR ==================\n"
        + "{}\n"
        + "============================================\n",
        testId, getContainerName(), stdOut == null ? "null" : stdOut, stdErr == null ? "null" : stdErr);

    if (exitCode != 0) {
      Path flinkLogPath = Paths.get(flinkRootDir, "log", "flink-*-client-*.log").toAbsolutePath();
      result = executor.execInContainer("bash", "-c", "cat " + flinkLogPath);

      LOG.error("Job exited with code {}, will print the client log now:\n"
          + "================== CLIENT LOG ==================\n"
          + "{}\n"
          + "================================================\n",
          exitCode, result.getStdout());
    }

    return exitCode;
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
