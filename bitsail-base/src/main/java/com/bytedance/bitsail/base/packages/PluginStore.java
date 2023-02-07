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

import com.bytedance.bitsail.base.version.VersionHolder;
import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.exception.CommonErrorCode;
import com.bytedance.bitsail.common.util.JsonSerializer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import lombok.Builder;
import lombok.Getter;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class PluginStore {
  private static final Logger LOG = LoggerFactory.getLogger(PluginStore.class);

  //in the future, we want to use plugin name to load plugin.
  @VisibleForTesting
  @Getter
  private final Map<String, Plugin> pluginNamesMapping;
  @VisibleForTesting
  @Getter
  private final Map<String, Plugin> libraryNamesMapping;

  private final Path pluginBaseDirPath;
  private final Path pluginMappingBaseDirPath;

  @Builder
  public PluginStore(Path pluginBaseDirPath, Path pluginMappingBaseDirPath) {
    this.pluginBaseDirPath = pluginBaseDirPath;
    this.pluginMappingBaseDirPath = pluginMappingBaseDirPath;
    this.pluginNamesMapping = Maps.newHashMap();
    this.libraryNamesMapping = Maps.newHashMap();

    loadPlugins();
  }

  private void loadPlugins() {
    LOG.info("Try load plugins from base plugin dir {}.", pluginBaseDirPath);
    LOG.info("Try load plugins from base plugin mapping dir {}.", pluginMappingBaseDirPath);

    if (Files.notExists(pluginMappingBaseDirPath)) {
      LOG.error("Nothing find under the path {}.", pluginMappingBaseDirPath);
      return;
    }

    try {
      Iterator<Path> pathIterator = Files.list(pluginMappingBaseDirPath)
          .iterator();

      while (pathIterator.hasNext()) {
        Path pluginMappingFile = pathIterator.next();
        Plugin plugin = JsonSerializer
            .deserialize(
                Files.readAllBytes(pluginMappingFile),
                Plugin.class
            );

        pluginNamesMapping.put(plugin.getPluginName(), plugin);
        Set<String> pluginClasses = Sets.newHashSet();
        if (StringUtils.isNotEmpty(plugin.getClassName())) {
          pluginClasses.add(plugin.getClassName());
        }
        if (CollectionUtils.isNotEmpty(plugin.getClassNames())) {
          pluginClasses.addAll(plugin.getClassNames());
        }
        pluginClasses.forEach(clazz -> libraryNamesMapping.put(clazz, plugin));

        LOG.debug("Finished load plugin {}.", plugin.getPluginName());
      }

    } catch (Exception e) {
      throw BitSailException.asBitSailException(
          CommonErrorCode.CONFIG_ERROR,
          "Plugins load failed.", e);
    }
  }

  public List<URL> getPluginUrls(String clazz) {
    //try load as plugin name
    Plugin plugin = pluginNamesMapping.get(clazz);
    if (Objects.nonNull(plugin)) {
      return plugin.getLibs()
          .stream()
          .map(this::checkExists)
          .collect(Collectors.toList());
    }

    //try load as library name
    plugin = libraryNamesMapping.get(clazz);
    if (Objects.isNull(plugin)) {
      LOG.warn("Class {} not register in mapping file.", clazz);
      return Collections.emptyList();
    }

    return plugin.getLibs()
        .stream()
        .map(this::checkExists)
        .collect(Collectors.toList());
  }

  private URL checkExists(String library) {
    Path location = pluginBaseDirPath.resolve(getRealLibrary(library));
    if (Files.notExists(location)) {
      throw BitSailException.asBitSailException(PluginErrorCode.PLUGIN_FILE_NOT_FOUND_ERROR,
          String.format("Plugin library not found in path %s.", location));
    }
    try {
      return location
          .toUri()
          .toURL();
    } catch (Exception e) {
      throw BitSailException.asBitSailException(PluginErrorCode.PLUGIN_COMMON_ERROR, e);
    }
  }

  private static String getRealLibrary(String library) {
    String buildVersion = VersionHolder.getHolder()
        .getBuildVersion();
    if (!StringUtils.contains(library, "${version}")) {
      return library;
    }
    return StringUtils.replace(library, "${version}", buildVersion);
  }
}
