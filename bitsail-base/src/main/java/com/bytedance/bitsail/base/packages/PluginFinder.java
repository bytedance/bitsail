/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.bytedance.bitsail.base.packages;

import com.bytedance.bitsail.base.component.ComponentBuilder;
import com.bytedance.bitsail.base.execution.ExecutionEnviron;
import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;

import java.io.Serializable;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public interface PluginFinder extends Serializable, ComponentBuilder<Void> {

  /**
   * Configure plugin explorer.
   */
  void configure(ExecutionEnviron execution,
                 BitSailConfiguration commonConfiguration);

  /**
   * find plugin instance from plugin finder.
   */
  <T> T findPluginInstance(String clazz, Object... parameters);

  /**
   * upload plugin to execution.
   */
  default void uploadPlugins(ExecutionEnviron execution,
                             List<URL> pluginUrls) {

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
