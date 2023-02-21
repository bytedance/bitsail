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

package com.bytedance.bitsail.test.e2e.executor.flink;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.configuration.BitSailSystemConfiguration;
import com.bytedance.bitsail.common.option.ReaderOptions;
import com.bytedance.bitsail.common.option.WriterOptions;
import com.bytedance.bitsail.test.e2e.base.transfer.TransferableFile;
import com.bytedance.bitsail.test.e2e.executor.AbstractExecutor;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerLoggerFactory;
import sun.awt.OSInfo;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.stream.Stream;

public abstract class AbstractFlinkExecutor extends AbstractExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractFlinkExecutor.class);

  protected static final String EXECUTOR_READY_MSG = "BitSail Test Executor Ready.";
  protected static final int EXECUTOR_READY_TIMEOUT = 30;

  /**
   * Whole test configuration.
   */
  protected BitSailConfiguration conf;

  /**
   * Flink docker.
   */
  protected GenericContainer<?> executor;

  @Override
  public void configure(BitSailConfiguration executorConf) {
    super.configure(executorConf);
    this.conf = executorConf;
  }

  @Override
  public void init() {
    String flinkDockerImage = getFlinkDockerImage();

    List<String> initCommands = Lists.newArrayList(
        "echo " + EXECUTOR_READY_MSG,
        "while true; do sleep 5; done"
    );
    executor = new GenericContainer<>(flinkDockerImage)
        .withNetwork(network)
        .withNetworkAliases(getContainerName())
        .withLogConsumer(new Slf4jLogConsumer(DockerLoggerFactory.getLogger(flinkDockerImage)).withSeparateOutputStreams())
        .withStartupAttempts(1)
        .withWorkingDirectory(EXECUTOR_ROOT_DIR.toAbsolutePath().toString())
        .withCommand("bash", "-c", String.join(" ;", initCommands))
        .waitingFor(new LogMessageWaitStrategy()
            .withRegEx(".*" + EXECUTOR_READY_MSG + ".*")
            .withStartupTimeout(Duration.ofSeconds(EXECUTOR_READY_TIMEOUT)))
        .withEnv(BitSailSystemConfiguration.BITSAIL_ENV_CONF_NAME, "bitsail-e2e.conf")
        .withCreateContainerCmdModifier(cmd -> cmd.withPlatform(OSInfo.getOSType().name()));

    for (TransferableFile file : transferableFiles) {
      copyToContainer(executor, file);
    }
  }

  public int run(String testId) throws IOException, InterruptedException {
    Startables.deepStart(Stream.of(executor)).join();

    String commands = String.join(" ", getExecCommand());
    LOG.info("Begin test: [{}], Container: [{}]\n"
            + "================ Test  Conf ================\n"
            + conf.desensitizedBeautify() + "\n"
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

  @Override
  protected void addJobConf(BitSailConfiguration executorConf) {
    executorConf.set(ReaderOptions.BaseReaderOptions.READER_PARALLELISM_NUM, 1);
    executorConf.set(WriterOptions.BaseWriterOptions.WRITER_PARALLELISM_NUM, 1);
    super.addJobConf(executorConf);
  }

  /**
   * Opensource flink docker image.
   */
  protected abstract String getFlinkDockerImage();

  /**
   * Commands for running bitsail e2e test.
   */
  protected List<String> getExecCommand() {
    return Lists.newArrayList(
        "bin/bitsail run",
        "--engine flink",
        "--execution-mode run",
        "--deployment-mode local",
        "--conf " + EXECUTOR_ROOT_DIR.resolve("jobConf.json").toAbsolutePath()
    );
  }
}
