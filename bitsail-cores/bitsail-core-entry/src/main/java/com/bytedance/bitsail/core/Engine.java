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

package com.bytedance.bitsail.core;

import com.bytedance.bitsail.base.execution.Mode;
import com.bytedance.bitsail.base.packages.PluginFinder;
import com.bytedance.bitsail.base.packages.PluginFinderFactory;
import com.bytedance.bitsail.base.statistics.VMInfo;
import com.bytedance.bitsail.base.version.VersionHolder;
import com.bytedance.bitsail.client.api.command.CommandArgsParser;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.option.CommonOptions;
import com.bytedance.bitsail.core.api.command.CoreCommandArgs;
import com.bytedance.bitsail.core.api.parser.ConfigurationHelper;
import com.bytedance.bitsail.core.api.program.Program;
import com.bytedance.bitsail.core.program.ProgramFactory;
import com.bytedance.bitsail.core.util.ExceptionTracker;

import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Engine {
  private static final Logger LOG = LoggerFactory.getLogger(Engine.class);
  private final Mode mode;
  @Getter
  private final BitSailConfiguration configuration;
  private final CoreCommandArgs coreCommandArgs;

  public Engine(String[] args) {
    coreCommandArgs = new CoreCommandArgs();
    String[] unknown = CommandArgsParser.parseArguments(args, coreCommandArgs);
    coreCommandArgs.setUnknownOptions(unknown);
    configuration = ConfigurationHelper.load(coreCommandArgs);
    LOG.info("BitSail configuration: {}", configuration.desensitizedBeautify());
    mode = Mode.getJobRunMode(configuration.get(CommonOptions.JOB_TYPE));
  }

  public static void main(String[] args) throws Throwable {
    Engine engine = new Engine(args);
    engine.start();
  }

  public void start() throws Throwable {
    VMInfo vmInfo = VMInfo.getVmInfo();
    if (null != vmInfo) {
      LOG.info(vmInfo.toString());
    }
    VersionHolder.print();
    try {
      run();
    } catch (Throwable e) {
      LOG.error("\n\nThe cause of the job failure maybe due to:\n" + ExceptionTracker.trace(e));
      exitWhenException(e);
    }
  }

  private void exitWhenException(Throwable e) throws Throwable {
    if (Mode.BATCH.equals(mode)) {
      System.exit(1);
    }
    throw e;
  }

  private <T> void run() throws Exception {
    //plugin load from original class loader.
    PluginFinder pluginFinder = PluginFinderFactory
        .getPluginFinder(configuration.get(CommonOptions.PLUGIN_FINDER_NAME));
    pluginFinder.configure(configuration);

    ClassLoader original = Thread.currentThread().getContextClassLoader();

    try {
      //set context class loader to plugin's class loader.
      Thread.currentThread().setContextClassLoader(pluginFinder.getClassloader());

      Program entryProgram = ProgramFactory.createEntryProgram(pluginFinder, coreCommandArgs, configuration);
      LOG.info("Final program: {}.", entryProgram.getComponentName());
      entryProgram.configure(pluginFinder, configuration, coreCommandArgs);

      if (entryProgram.validate()) {
        entryProgram.submit();
      }
    } finally {
      if (configuration.fieldExists(CommonOptions.SLEEP_TIME)) {
        Thread.sleep(configuration.get(CommonOptions.SLEEP_TIME));
      }
      //reset context classloader to original.
      Thread.currentThread().setContextClassLoader(original);
    }
  }
}
