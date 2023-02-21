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

package com.bytedance.bitsail.connector.larksheet.util;

import org.junit.Assert;
import org.junit.Test;

public class LarkSheetUtilTest {

  @Test
  public void testNumberToSequence() {
    String seq = LarkSheetUtil.numberToSequence(28);
    Assert.assertEquals("AB", seq);

    seq = LarkSheetUtil.numberToSequence(26);
    Assert.assertEquals("Z", seq);
  }
}
