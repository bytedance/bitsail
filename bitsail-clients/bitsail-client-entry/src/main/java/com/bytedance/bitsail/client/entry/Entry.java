/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bytedance.bitsail.client.entry;

import com.bytedance.bitsail.client.api.command.BaseCommandArgs;
import com.bytedance.bitsail.client.api.command.BaseCommandArgsWithUnknownOptions;
import com.bytedance.bitsail.client.api.command.CommandAction;
import com.bytedance.bitsail.client.api.command.CommandArgsParser;
import com.bytedance.bitsail.client.api.engine.EngineRunner;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.configuration.ConfigParser;

import com.beust.jcommander.internal.Maps;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;

/**
 * Created 2022/8/1
 */
public class Entry {
  private static final Logger LOG = LoggerFactory.getLogger(Entry.class);

  private static final Map<String, EngineRunner> RUNNERS = Maps.newHashMap();
  private static final Class<EngineRunner> ENGINE_SPI_CLASS = EngineRunner.class;

  private static final int ERROR_EXIT_CODE = 1;

  private static final Object LOCK = new Object();
  private static Process process;
  private static volatile boolean running;

  private static void loadAllEngines() {
    for (EngineRunner runner : ServiceLoader.load(ENGINE_SPI_CLASS)) {
      String engineName = runner.engineName();
      LOG.info("Load engine {} from classpath.", engineName);
      RUNNERS.put(StringUtils.upperCase(engineName), runner);
    }
  }

  public static void main(String[] args) {
    loadAllEngines();

    try {
      int exit = handleCommand(args);
      System.exit(exit);
    } catch (Exception e) {
      LOG.error("Exception occurred when run command .", e);
      System.exit(ERROR_EXIT_CODE);
    }
  }

  @SuppressWarnings("checkstyle:RegexpSingleline")
  private static int handleCommand(String[] args) throws IOException, InterruptedException {
    if (args.length < 1) {
      CommandArgsParser.printHelp();
      System.out.println("Please specify an action. Supported action are " + CommandAction.RUN_COMMAND);
      return ERROR_EXIT_CODE;
    }
    BaseCommandArgsWithUnknownOptions baseCommandArgsWithUnknownOptions = buildCommandArgs(args);
    ProcessBuilder processBuilder = buildProcessBuilder(baseCommandArgsWithUnknownOptions);
    return startProcessBuilder(processBuilder, baseCommandArgsWithUnknownOptions.getBaseCommandArgs());
  }

  static BaseCommandArgsWithUnknownOptions buildCommandArgs(String[] args) {
    if (args.length < 1) {
      CommandArgsParser.printHelp();
      throw new IllegalArgumentException("Please specify an action. Supported action are " + CommandAction.RUN_COMMAND);
    }
    final String mainCommand = args[0];
    final String[] params = Arrays.copyOfRange(args, 1, args.length);
    BaseCommandArgs baseCommandArgs = new BaseCommandArgs();
    String[] unknownOptions = CommandArgsParser.parseArguments(params, baseCommandArgs);
    baseCommandArgs.setMainAction(mainCommand);
    return BaseCommandArgsWithUnknownOptions.builder().baseCommandArgs(baseCommandArgs).unknownOptions(unknownOptions).build();
  }

  private static ProcessBuilder buildProcessBuilder(BaseCommandArgsWithUnknownOptions baseCommandArgsWithUnknownOptions) {
    BaseCommandArgs baseCommandArgs = baseCommandArgsWithUnknownOptions.getBaseCommandArgs();
    BitSailConfiguration jobConfiguration =
        ConfigParser.fromRawConfPath(baseCommandArgs.getJobConf());

    String engineName = baseCommandArgs.getEngineName();
    LOG.info("Input argument engine name: {}.", engineName);
    EngineRunner engineRunner = RUNNERS.get(StringUtils.upperCase(engineName));
    if (Objects.isNull(engineRunner)) {
      throw new IllegalArgumentException(String.format("Engine %s not support now.", engineName));
    }
    engineRunner.addEngineClasspath();

    ProcessBuilder procBuilder = engineRunner.getProcBuilder(
        jobConfiguration,
        baseCommandArgs,
        baseCommandArgsWithUnknownOptions.getUnknownOptions());
    LOG.info("Engine {}'s command: {}.", baseCommandArgs.getEngineName(), procBuilder.command());
    return procBuilder;
  }

  private static int startProcessBuilder(ProcessBuilder procBuilder, BaseCommandArgs runCommandArgs) throws IOException, InterruptedException {
    procBuilder
        .redirectOutput(ProcessBuilder.Redirect.INHERIT)
        .redirectError(ProcessBuilder.Redirect.INHERIT);

    Thread hook = new Thread(() -> {
      synchronized (LOCK) {
        if (running) {
          if (Objects.nonNull(process) && process.isAlive()) {
            LOG.info("Shutdown the running proc with pid = {}.", process);
            process.destroy();
          }
        }
      }
    });

    Runtime.getRuntime().addShutdownHook(hook);
    int i = internalRunProcess(procBuilder, runCommandArgs);
    Runtime.getRuntime().removeShutdownHook(hook);
    return i;
  }

  private static int internalRunProcess(ProcessBuilder procBuilder,
                                        BaseCommandArgs runCommandArgs) throws IOException, InterruptedException {
    synchronized (LOCK) {
      running = true;
      process = procBuilder.start();
    }

    if (!runCommandArgs.isDetach()) {
      return process.waitFor();
    }
    return ERROR_EXIT_CODE;
  }
}
