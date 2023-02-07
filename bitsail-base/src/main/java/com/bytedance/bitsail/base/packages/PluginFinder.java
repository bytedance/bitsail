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

import com.bytedance.bitsail.base.component.ComponentBuilder;
import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;

import java.io.Serializable;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Set;

public interface PluginFinder extends Serializable, ComponentBuilder<Void> {

  /**
   * Configure plugin finder.
   */
  void configure(BitSailConfiguration commonConfiguration);

  /**
   * find plugin instance from plugin finder.
   */
  <T> T findPluginInstance(String clazz, Object... parameters);

  /**
   * Load plugin jar to the plugin finder.
   */
  void loadPlugin(String plugin);

  /**
   * Get all founded plugins
   */
  default Set<URL> getFoundedPlugins() {
    return Collections.emptySet();
  }

  default ClassLoader getClassloader() {
    return Thread.currentThread().getContextClassLoader();
  }

  /**
   * Get framework entry dir.
   * todo remove to common module in future.
   */
  default Path getFrameworkEntryDir() {
    try {
      String entry = Paths.get(PluginFinder.class
          .getProtectionDomain()
          .getCodeSource()
          .getLocation()
          .toURI()).toString();

      return Paths.get(entry).getParent();
    } catch (Exception e) {
      throw BitSailException.asBitSailException(PluginErrorCode.PLUGIN_COMMON_ERROR, e);
    }
  }

}
