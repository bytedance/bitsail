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

import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.option.CommonOptions;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Set;

public class LocalFSPluginFinder implements PluginFinder {
  private static final Logger LOG = LoggerFactory.getLogger(LocalFSPluginFinder.class);

  private static final String DEFAULT_PLUGIN_FINDER_NAME = "localFS";
  private List<PluginStore> pluginStores;
  private URLClassLoader pluginClassloader;

  @Override
  public void configure(BitSailConfiguration commonConfiguration) {
    this.pluginStores = createPluginStores(commonConfiguration);
    this.pluginClassloader = createPluginClassloader();
  }

  protected List<PluginStore> createPluginStores(BitSailConfiguration commonConfiguration) {
    List<PluginStore> pluginStores = Lists.newArrayList();
    String frameworkBaseDir = commonConfiguration
        .getUnNecessaryOption(CommonOptions.JOB_PLUGIN_ROOT_PATH, getFrameworkEntryDir().toString());

    String pluginDirName = commonConfiguration.get(CommonOptions.JOB_PLUGIN_DIR_NAME);
    String pluginMappingDirName = commonConfiguration.get(CommonOptions.JOB_PLUGIN_MAPPING_DIR_NAME);

    Path frameworkBaseDirPath = Paths.get(frameworkBaseDir);

    pluginStores.add(PluginStore.builder()
        .pluginBaseDirPath(frameworkBaseDirPath.resolve(pluginDirName))
        .pluginMappingBaseDirPath(frameworkBaseDirPath.resolve(pluginMappingDirName))
        .build());

    String engineDirName = commonConfiguration.get(CommonOptions.JOB_ENGINE_DIR_NAME);
    String engineMappingDirName = commonConfiguration.get(CommonOptions.JOB_ENGINE_MAPPING_DIR_NAME);

    pluginStores.add(PluginStore.builder()
        .pluginBaseDirPath(frameworkBaseDirPath.resolve(engineDirName))
        .pluginMappingBaseDirPath(frameworkBaseDirPath.resolve(engineMappingDirName))
        .build());

    return pluginStores;
  }

  protected URLClassLoader createPluginClassloader() {
    return URLClassLoader.newInstance(new URL[] {}, Thread.currentThread()
        .getContextClassLoader());
  }

  @Override
  public <T> T findPluginInstance(String canonicalName, Object... parameters) {
    return findPluginInstance(canonicalName, false, parameters);
  }

  private <T> T findPluginInstance(String canonicalName, boolean failOnMiss, Object... parameters) {
    Class<?> clazz;
    try {
      clazz = pluginClassloader.loadClass(canonicalName);
      return newInstance(clazz, parameters);
    } catch (ClassNotFoundException e) {
      if (failOnMiss) {
        throw BitSailException.asBitSailException(
            PluginErrorCode.PLUGIN_NOT_FOUND_ERROR,
            String.format("The class %s not exists in framework.", canonicalName));
      }
    }

    LOG.warn("The class {} not exists in the class loader, try load from plugin store.", canonicalName);
    loadPlugin(canonicalName);
    return findPluginInstance(canonicalName, true, parameters);
  }

  @Override
  public void loadPlugin(String canonicalName) {
    List<URL> pluginUrls = null;
    for (PluginStore pluginStore : pluginStores) {
      pluginUrls = pluginStore.getPluginUrls(canonicalName);
      if (CollectionUtils.isNotEmpty(pluginUrls)) {
        break;
      }
    }

    if (CollectionUtils.isEmpty(pluginUrls)) {
      throw BitSailException.asBitSailException(
          PluginErrorCode.PLUGIN_NOT_FOUND_ERROR,
          String.format("The class %s not exists in plugin store.", canonicalName));
    }

    tryAddPluginToClassloader(pluginClassloader, pluginUrls);
  }

  @Override
  public Set<URL> getFoundedPlugins() {
    return Sets.newHashSet(pluginClassloader.getURLs());
  }

  @Override
  public ClassLoader getClassloader() {
    return pluginClassloader;
  }

  private static void tryAddPluginToClassloader(URLClassLoader classloader,
                                                List<URL> pluginUrls) {
    try {
      Method addUrlMethod = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
      addUrlMethod.setAccessible(true);

      for (URL pluginUrl : pluginUrls) {
        addUrlMethod.invoke(classloader, pluginUrl);
        LOG.info("Plugin class loader add plugin url: {}.", pluginUrl);
      }

    } catch (Exception e) {
      //ignore
    }
  }

  @SuppressWarnings("unchecked")
  private <T> T newInstance(Class<?> clazz, Object... parameters) {
    ClassLoader original = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(pluginClassloader);
    try {
      try {
        Class<?>[] clazzParameters = new Class[parameters.length];
        for (int index = 0; index < parameters.length; index++) {
          clazzParameters[index] = parameters[index].getClass();
        }
        Constructor<?> constructor = clazz.getDeclaredConstructor(clazzParameters);
        constructor.setAccessible(true);
        return (T) constructor.newInstance(parameters);
      } catch (Exception e) {
        throw BitSailException.asBitSailException(PluginErrorCode.PLUGIN_NEW_INSTANCE_ERROR, e);
      }
    } finally {
      Thread.currentThread().setContextClassLoader(original);
    }
  }

  @Override
  public String getComponentName() {
    return DEFAULT_PLUGIN_FINDER_NAME;
  }
}
