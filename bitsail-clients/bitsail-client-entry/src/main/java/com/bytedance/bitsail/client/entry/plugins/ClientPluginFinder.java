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

package com.bytedance.bitsail.client.entry.plugins;

import com.bytedance.bitsail.base.packages.LocalFSPluginFinder;
import com.bytedance.bitsail.base.packages.PluginStore;
import com.bytedance.bitsail.client.api.utils.PackageResolver;
import com.bytedance.bitsail.client.entry.option.ClientCommonOption;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.option.CommonOptions;

import com.google.common.collect.Lists;

import java.net.URLClassLoader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class ClientPluginFinder extends LocalFSPluginFinder {

  private final URLClassLoader clientPluginClassloader;

  public ClientPluginFinder(URLClassLoader classLoader) {
    this.clientPluginClassloader = classLoader;
  }

  @Override
  protected List<PluginStore> createPluginStores(BitSailConfiguration commonConfiguration) {
    List<PluginStore> pluginStores = Lists.newArrayList();

    String frameworkBaseDir = commonConfiguration
        .getUnNecessaryOption(CommonOptions.JOB_PLUGIN_ROOT_PATH,
            PackageResolver.getLibraryDir().toUri().getPath());

    String pluginDirName = commonConfiguration.get(ClientCommonOption.CLIENT_ENGINE_DIR_NAME);
    String pluginMappingDirName = commonConfiguration.get(ClientCommonOption.CLIENT_ENGINE_MAPPING_DIR_NAME);

    Path frameworkPath = Paths.get(frameworkBaseDir);
    pluginStores.add(PluginStore.builder()
        .pluginBaseDirPath(frameworkPath.resolve(pluginDirName))
        .pluginMappingBaseDirPath(frameworkPath.resolve(pluginMappingDirName))
        .build());

    return pluginStores;
  }

  @Override
  protected URLClassLoader createPluginClassloader() {
    return clientPluginClassloader;
  }
}
