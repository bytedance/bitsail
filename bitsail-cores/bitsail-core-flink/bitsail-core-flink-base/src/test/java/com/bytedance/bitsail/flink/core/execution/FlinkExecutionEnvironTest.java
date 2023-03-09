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

package com.bytedance.bitsail.flink.core.execution;

import com.bytedance.bitsail.base.execution.Mode;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;

import com.google.common.collect.Sets;
import org.apache.flink.configuration.ConfigUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.net.URL;
import java.util.HashSet;
import java.util.Set;

public class FlinkExecutionEnvironTest {

  private FlinkExecutionEnviron environ;

  @Before
  public void initFlinkExecutionEnviron() {
    environ = new FlinkExecutionEnviron();
    environ.configure(Mode.STREAMING, null, BitSailConfiguration.newDefault());
  }

  @Test
  public void testAddPluginToExecution() throws Throwable {
    URL fakeFile = new URL("file:///tmp/urls/a.jar");
    Set<URL> urls = Sets.newHashSet(fakeFile);
    environ.addPluginToExecution(urls);

    Configuration configuration = environ.getFlinkConfiguration();
    Set<URI> classpath = new HashSet<>(ConfigUtils.decodeListFromConfig(
        configuration, PipelineOptions.JARS, URI::create));
    Assert.assertTrue(classpath.contains(fakeFile.toURI()));
  }
}
