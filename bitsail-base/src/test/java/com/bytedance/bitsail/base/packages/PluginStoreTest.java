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

import org.apache.commons.collections.CollectionUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PluginStoreTest {

  private PluginStore pluginStore;

  @Before
  public void init() throws URISyntaxException {
    String confPathRoot = Paths.get(PluginStoreTest.class.getResource("/classloader/").toURI()).toString();
    pluginStore = PluginStore.builder()
        .pluginBaseDirPath(Paths.get(confPathRoot, "plugin"))
        .pluginMappingBaseDirPath(Paths.get(confPathRoot, "plugin_conf"))
        .build();
  }

  @Test
  public void testPluginNamesMapping() {
    Map<String, Plugin> pluginNamesMapping = pluginStore.getPluginNamesMapping();
    assertEquals(4, pluginNamesMapping.size());
  }

  @Test
  public void testLibraryNamesMapping() {
    Map<String, Plugin> pluginNamesMapping = pluginStore.getLibraryNamesMapping();
    assertEquals(2, pluginNamesMapping.size());
  }

  @Test
  public void testGetPluginUrls() {
    List<URL> pluginLibs = pluginStore.getPluginUrls("test1");
    assertEquals(3, pluginLibs.size());
    assertTrue(pluginLibs.get(0).getPath().endsWith("test1"));

    pluginLibs = pluginStore.getPluginUrls("com.bytedance.bitsail.batch.test2");
    assertEquals(3, pluginLibs.size());
  }

  @Test
  public void testLoadPlugin() {
    List<URL> pluginUrls = pluginStore.getPluginUrls("test3");
    Assert.assertEquals(pluginUrls.size(), 1);
  }

  @Test
  public void getGetPluginUrlsNotExists() {
    List<URL> pluginUrls = pluginStore.getPluginUrls("com.bytedance.bitsail.batch.jdbc.test");
    Assert.assertTrue(CollectionUtils.isEmpty(pluginUrls));
  }
}