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

package com.bytedance.bitsail.entry.flink.utils;

import lombok.SneakyThrows;
import org.apache.flink.configuration.Configuration;
import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;

import static com.bytedance.bitsail.entry.flink.utils.FlinkPackageResolver.getFlinkConfDir;
import static com.bytedance.bitsail.entry.flink.utils.FlinkPackageResolver.getFlinkLibDir;
import static com.bytedance.bitsail.entry.flink.utils.FlinkPackageResolver.loadFlinkConfiguration;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FlinkPackageResolverTest {
  @Test
  public void testGetFlinkConfDir() {
    Path path = getResourcePath("test_dir");
    Path confPath = getResourcePath("test_dir/conf");
    assertEquals(confPath, getFlinkConfDir(path));
  }

  @Test
  public void testGetFlinkLibDir() {
    Path path = getResourcePath("test_dir");
    Path libPath = getResourcePath("test_dir/lib");
    assertEquals(libPath, getFlinkLibDir(path));
  }

  @Test
  public void testDefaultConf() {
    Path path = getResourcePath("conf");
    Configuration flinkConfiguration = loadFlinkConfiguration(path);
    assertEquals("1", flinkConfiguration.getString("parallelism.default", null));
  }

  @Test
  public void testUserDefinedConf() {
    Path path = getResourcePath("test_dir/conf");
    Configuration flinkConfiguration = loadFlinkConfiguration(path);
    assertEquals("2", flinkConfiguration.getString("parallelism.default", null));
  }

  @Test
  public void testNoConf() {
    Path path = getResourcePath("test_dir_no_conf_files/conf");
    Configuration flinkConfiguration = loadFlinkConfiguration(path);
    assertTrue(flinkConfiguration.toMap().isEmpty());
  }

  @SneakyThrows
  private Path getResourcePath(String resource) {
    return Paths.get(Objects.requireNonNull(FlinkPackageResolverTest.class
            .getClassLoader()
            .getResource(resource))
            .toURI()
    );
  }
}
