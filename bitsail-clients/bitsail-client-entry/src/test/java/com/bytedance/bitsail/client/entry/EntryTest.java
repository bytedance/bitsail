/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bytedance.bitsail.client.entry;

import com.bytedance.bitsail.client.api.command.BaseCommandArgsWithUnknownOptions;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class EntryTest {

  @Test(expected = IllegalArgumentException.class)
  public void testBuildCommandArgsWithEmptyArgs() {
    String[] args = {};
    Entry.buildCommandArgs(args);
  }

  @Test
  public void testBuildCommandArgs() {
    String[] args = new String[] {"run", "--engine", "flink", "-d", "-sae"};
    BaseCommandArgsWithUnknownOptions commandArgsWithUnknownOptions =
        Entry.buildCommandArgs(args);
    assertEquals(commandArgsWithUnknownOptions.getUnknownOptions().length, 1);
    assertEquals(commandArgsWithUnknownOptions.getBaseCommandArgs().getEngineName(), "flink");
    assertEquals(commandArgsWithUnknownOptions.getBaseCommandArgs().getMainAction(), "run");
  }
}
