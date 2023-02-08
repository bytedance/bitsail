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

package com.bytedance.bitsail.base.metrics.manager;

import org.junit.Assert;
import org.junit.Test;

public class LockFreeSlidingWindowReservoirTest {

  @Test
  public void testSnapshot() {
    int updateCount = 0;
    int windowSize = 3;
    LockFreeSlidingWindowReservoir reservoir = new LockFreeSlidingWindowReservoir(windowSize);

    reservoir.update(0);
    Assert.assertEquals(1, reservoir.size());

    reservoir.update(1);
    Assert.assertEquals(2, reservoir.size());

    reservoir.update(2);
    Assert.assertEquals(3, reservoir.size());

    Assert.assertArrayEquals(new long[] {0, 1, 2}, reservoir.getSnapshot().getValues());

    reservoir.update(3);
    reservoir.update(4);
    reservoir.update(5);
    Assert.assertEquals(3, reservoir.size());
    Assert.assertArrayEquals(new long[] {3, 4, 5}, reservoir.getSnapshot().getValues());
  }
}
