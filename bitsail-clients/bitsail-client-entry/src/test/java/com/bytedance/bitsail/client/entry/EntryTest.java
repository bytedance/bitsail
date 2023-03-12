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

package com.bytedance.bitsail.client.entry;

import com.bytedance.bitsail.client.api.command.BaseCommandArgs;

import com.github.stefanbirkner.systemlambda.SystemLambda;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Base64;

@Slf4j
public class EntryTest {

  @Test
  public void testBuildCommandArgs() {
    String[] args = new String[] {"run", "--engine", "flink", "-d", "-sae"};
    BaseCommandArgs baseCommandArgs = Entry.loadCommandArguments(args);
    Assert.assertEquals(baseCommandArgs.getUnknownOptions().length, 1);
    Assert.assertEquals(baseCommandArgs.getEngineName(), "flink");
    Assert.assertEquals(baseCommandArgs.getMainAction(), "run");
  }

  @Test
  public void testBuildCommandArgsCancel() {
    String[] args = new String[] {"cancel", "--engine", "flink"};
    BaseCommandArgs baseCommandArgs = Entry.loadCommandArguments(args);
    Assert.assertEquals(baseCommandArgs.getUnknownOptions().length, 0);
    Assert.assertEquals(baseCommandArgs.getEngineName(), "flink");
    Assert.assertEquals(baseCommandArgs.getMainAction(), "cancel");
  }

  @Test
  public void testBuildFakeEngine() throws Exception {
    String jobConfPath = Paths.get(
        EntryTest.class.getResource("/test_job_conf.json").toURI()
    ).toFile().getAbsolutePath();
    String[] args = new String[] {"run", "--engine", "fake", "--conf", jobConfPath};
    SystemLambda.catchSystemExit(() -> Entry.main(args));
  }

  @Test
  public void testBuildFakeEngineWithCancel() throws Exception {
    String[] args = new String[] {"stop", "--engine", "fake"};
    SystemLambda.catchSystemExit(() -> Entry.main(args));
  }

  @Test
  public void testBuildFakeEngineWithConfInBase64() throws Exception {
    Path jobConfPath = Paths.get(
            EntryTest.class.getResource("/test_job_conf.json").toURI()
    ).toFile().toPath();
    byte[] fileContent = Files.readAllBytes(jobConfPath);
    String jobConfInBase64 = Base64.getEncoder().encodeToString(fileContent);
    String[] args = new String[] {"run", "--engine", "fake", "--conf-in-base64", jobConfInBase64};
    SystemLambda.catchSystemExit(() -> Entry.main(args));
  }
}
