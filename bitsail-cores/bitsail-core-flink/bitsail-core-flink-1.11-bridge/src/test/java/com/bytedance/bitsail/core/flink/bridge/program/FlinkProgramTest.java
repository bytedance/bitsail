/*
 *       Copyright 2022-2023 Bytedance Ltd. and/or its affiliates.
 *
 *       Licensed under the Apache License, Version 2.0 (the "License");
 *       you may not use this file except in compliance with the License.
 *       You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *       Unless required by applicable law or agreed to in writing, software
 *       distributed under the License is distributed on an "AS IS" BASIS,
 *       WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *       See the License for the specific language governing permissions and
 *       limitations under the License.
 */

package com.bytedance.bitsail.core.flink.bridge.program;

import com.bytedance.bitsail.base.packages.PluginFinder;
import com.bytedance.bitsail.base.packages.PluginFinderFactory;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.option.CommonOptions;
import com.bytedance.bitsail.core.api.command.CoreCommandArgs;
import com.bytedance.bitsail.core.api.program.UnifiedProgram;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Objects;

public class FlinkProgramTest {

  private static final Logger LOG = LoggerFactory.getLogger(FlinkProgramTest.class);

  @Test
  public void submit() throws Exception {

    BitSailConfiguration globalConfiguration = loadConf("bitsail_connector_unified_conf.json");
    LOG.info("Final Configuration: {}.\n", globalConfiguration.desensitizedBeautify());
    CoreCommandArgs coreCommandArgs = new CoreCommandArgs();
    coreCommandArgs.setEngineName("flink");
    UnifiedProgram unifiedProgram = new FlinkProgram();
    PluginFinder pluginFinder = PluginFinderFactory.getPluginFinder(globalConfiguration.get(CommonOptions.PLUGIN_FINDER_NAME));
    pluginFinder.configure(globalConfiguration);
    unifiedProgram.configure(pluginFinder, globalConfiguration, coreCommandArgs);
    unifiedProgram.submit();
  }

  @Test
  public void testSubmitTransform() throws Exception {
    BitSailConfiguration globalConfiguration = loadConf("bitsail_transform_conf.json");
    LOG.info("Final Configuration: {}.\n", globalConfiguration.desensitizedBeautify());
    CoreCommandArgs coreCommandArgs = new CoreCommandArgs();
    coreCommandArgs.setEngineName("flink");
    UnifiedProgram unifiedProgram = new FlinkProgram();
    PluginFinder pluginFinder = PluginFinderFactory.getPluginFinder(globalConfiguration.get(CommonOptions.PLUGIN_FINDER_NAME));
    pluginFinder.configure(globalConfiguration);
    unifiedProgram.configure(pluginFinder, globalConfiguration, coreCommandArgs);
    unifiedProgram.submit();
  }

  private static BitSailConfiguration loadConf(String path) throws URISyntaxException, IOException {
    ClassLoader classLoader = FlinkProgram.class.getClassLoader();
    URL resource = classLoader.getResource(path);
    if (Objects.isNull(resource)) {
      throw new IllegalArgumentException(String.format("Resources name: %s not found in classpath.",
          path));
    }
    return BitSailConfiguration.from(new String(Files.readAllBytes(Paths.get(resource.toURI()))));
  }
}