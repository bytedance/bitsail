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

package com.bytedance.bitsail.test.e2e.executor.generic;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.test.e2e.base.transfer.TransferableFile;
import com.bytedance.bitsail.test.e2e.executor.AbstractExecutor;

import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerLoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.stream.Stream;

public class GenericExecutor extends AbstractExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(GenericExecutor.class);

  protected static final String EXECUTOR_READY_MSG = "Generic Executor Ready.";
  protected static final int EXECUTOR_READY_TIMEOUT = 30;

  protected GenericExecutorSetting setting;

  /**
   * Whole test configuration.
   */
  protected BitSailConfiguration conf;

  /**
   * Docker.
   */
  protected GenericContainer<?> executor;

  public GenericExecutor(GenericExecutorSetting setting) {
    this.setting = setting;
  }

  @Override
  public void configure(BitSailConfiguration executorConf) {
    this.conf = executorConf;
  }

  @Override
  public String getContainerName() {
    return setting.getExecutorName();
  }

  @Override
  public void init() {
    List<String> initCommands = Lists.newArrayList(
        "echo " + EXECUTOR_READY_MSG,
        "while true; do sleep 5; done"
    );
    executor = new GenericContainer<>(setting.getExecutorImage())
        .withNetwork(network)
        .withNetworkAliases(getContainerName())
        .withLogConsumer(new Slf4jLogConsumer(
            DockerLoggerFactory.getLogger(setting.getExecutorImage())).withSeparateOutputStreams())
        .withStartupAttempts(1)
        .withWorkingDirectory(EXECUTOR_ROOT_DIR.toAbsolutePath().toString())
        .withCommand("bash", "-c", String.join(" ;", initCommands))
        .waitingFor(new LogMessageWaitStrategy()
            .withRegEx(".*" + EXECUTOR_READY_MSG + ".*")
            .withStartupTimeout(Duration.ofSeconds(EXECUTOR_READY_TIMEOUT)));

    for (TransferableFile file : transferableFiles) {
      copyToContainer(executor, file);
    }
  }

  @Override
  public int run(String testId) throws Exception {
    Startables.deepStart(Stream.of(executor)).join();

    String commands = String.join(" ", setting.getExecCommands());
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

    if (exitCode != 0 && CollectionUtils.isNotEmpty(setting.getFailureHandleCommands())) {
      commands = String.join(" ", setting.getFailureHandleCommands());
      result = executor.execInContainer("bash", "-c", commands);

      LOG.error("Job exited with code {}, will execute the failure handle commands now:\n"
              + "=========== FAILURE HANDLE COMMANDS ============\n"
              + "{}\n"
              + "==================== STDOUT ====================\n"
              + "{}\n"
              + "==================== STDERR ====================\n"
              + "{}\n"
              + "================================================\n",
          exitCode, commands, result.getStdout(), result.getStderr());
    }

    return exitCode;
  }

  @Override
  public String getClientEngineModule() {
    return setting.getClientModule();
  }

  @Override
  public String getCoreEngineModule() {
    return setting.getCoreModule();
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
    BitSailConfiguration globalConf = setting.getGlobalJobConf();
    executorConf.merge(globalConf, true);
    super.addJobConf(executorConf);
  }
}
