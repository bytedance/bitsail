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

package com.bytedance.bitsail.core;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.option.CommonOptions;
import com.bytedance.bitsail.core.program.FakeProgram;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.Base64;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class EngineTest {
  private static final Logger LOG = LoggerFactory.getLogger(EngineTest.class);

  @Test
  public void testBase64ArgsToConfig() {
    String jobConf = "{\n" +
        "    \"key\":\"value\"\n" +
        "}";
    Engine engine = new Engine(
        new String[] {
            "-xjob_conf_in_base64", Base64.getEncoder().encodeToString(jobConf.getBytes())
        });
    assertTrue(engine.getConfiguration().fieldExists("key"));
    assertFalse(engine.getConfiguration().fieldExists("key1"));
  }

  @Test
  public void testConfPathToConfig() throws URISyntaxException {
    String confPath = Paths.get(this.getClass().getResource("/conf.json").toURI()).toString();
    Engine engine = new Engine(
        new String[] {
            "-xjob_conf", confPath
        });
    assertTrue(engine.getConfiguration().fieldExists("key"));
    assertFalse(engine.getConfiguration().fieldExists("key1"));
  }

  @Test
  public void testRunEngine() {
    BitSailConfiguration jobConf = BitSailConfiguration.newDefault();
    jobConf.set(CommonOptions.JOB_ID, 123L);
    jobConf.set(CommonOptions.USER_NAME, "test_user");
    jobConf.set(CommonOptions.PLUGIN_FINDER_NAME, new FakePluginFinder().getComponentName());
    String base64JobConf = Base64.getEncoder().encodeToString(jobConf.toString().getBytes());

    Throwable caught = null;
    try {
      Engine.main(new String[] {
          "-xjob_conf_in_base64", base64JobConf,
          "--engine", new FakeProgram().getComponentName()
      });
    } catch (Throwable t) {
      caught = t;
      LOG.error("Catch throwable when run ening.", t);
      t.printStackTrace();
    }

    Assert.assertNull(caught);
  }
}
