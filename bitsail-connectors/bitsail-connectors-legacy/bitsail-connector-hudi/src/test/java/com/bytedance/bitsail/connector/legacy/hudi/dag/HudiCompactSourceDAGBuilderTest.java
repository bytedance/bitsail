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

package com.bytedance.bitsail.connector.legacy.hudi.dag;

import org.junit.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class HudiCompactSourceDAGBuilderTest {
  @Test
  public void testParallelismCalculation() {
    assertEquals(1, HudiCompactSourceDAGBuilder.calculateParallelism(1, 2, 2));
    assertEquals(2, HudiCompactSourceDAGBuilder.calculateParallelism(11, 2, 2));
    assertEquals(6, HudiCompactSourceDAGBuilder.calculateParallelism(11, 2, 10));
  }
}
