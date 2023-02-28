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
import com.bytedance.bitsail.client.api.engine.EngineRunner;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;

/**
 * A fake engine for UT.
 */
public class FakeEngine implements EngineRunner {

  @Override
  public void initializeEngine(BitSailConfiguration sysConfiguration) {
    // do nothing
  }

  @Override
  public ProcessBuilder getProcBuilder(BaseCommandArgs baseCommandArgs) {
    return new ProcessBuilder("bash", "-c", "echo fake-engine!");
  }

  @Override
  public String engineName() {
    return "fake";
  }
}
