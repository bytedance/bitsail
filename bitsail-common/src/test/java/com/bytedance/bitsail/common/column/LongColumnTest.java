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

package com.bytedance.bitsail.common.column;

import org.junit.Test;

import java.time.ZoneId;

import static org.junit.Assert.assertEquals;

public class LongColumnTest {

  @Test
  public void asDate() {
    assertEquals("2019-04-10T17:23:58+08:00[Asia/Shanghai]",
        new LongColumn(1554888238).asDate().toInstant().atZone(ZoneId.of("Asia/Shanghai")).toString());
  }
}