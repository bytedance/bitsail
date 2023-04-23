/*
 * Copyright 2022-2023 Bytedance Ltd. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.bytedance.bitsail.core.flink.bridge.transform.delegate;

import com.bytedance.bitsail.common.row.Row;

import org.apache.flink.api.java.functions.KeySelector;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class RowKeySelectorTest {
  @Test
  public void testBitSailRowSelector() throws Exception {
    KeySelector<Object, Object> keySelector = new RowKeySelector<>(0);
    Row row = new Row(2);
    row.setField(0, "key");
    assertEquals("key", keySelector.getKey(row));
  }

  @Test
  public void testFlinkRowSelector() throws Exception {
    KeySelector<Object, Object> keySelector = new RowKeySelector<>(0);
    org.apache.flink.types.Row row = new org.apache.flink.types.Row(2);
    row.setField(0, "key");
    assertEquals("key", keySelector.getKey(row));
  }
}
