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

package com.bytedance.bitsail.base.packages;

import com.bytedance.bitsail.base.execution.ExecutionEnviron;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.option.CommonOptions;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.Objects;

public class LocalFSPluginFinderTest {

  private final String[] plugins = {"plugin/test1", "plugin/test2", "plugin/test3"};
  private BitSailConfiguration jobConf;
  private ExecutionEnviron mockedEnv;

  @Before
  public void init() throws URISyntaxException {
    jobConf = BitSailConfiguration.newDefault();
    jobConf.set(CommonOptions.JOB_PLUGIN_DIR_NAME, "plugin");
    jobConf.set(CommonOptions.JOB_PLUGIN_MAPPING_DIR_NAME, "plugin_conf");
    jobConf.set(CommonOptions.STATIC_LIB_DIR, "plugin");
    jobConf.set(CommonOptions.STATIC_LIB_CONF_FILE, "static_lib.json");
    jobConf.set(CommonOptions.JOB_PLUGIN_ROOT_PATH,
        Objects.requireNonNull(Paths.get(LocalFSPluginFinder.class.getResource("/classloader/").toURI()).toString()));
  }

  @Test
  public void testLoadPluginInstance() {
    LocalFSPluginFinder pluginFinder = new LocalFSPluginFinder();
    pluginFinder.configure(jobConf);
    Object instance = pluginFinder.findPluginInstance(
        "com.bytedance.bitsail.base.packages.LocalFSPluginFinder");
    Assert.assertNotNull(instance);
  }
}