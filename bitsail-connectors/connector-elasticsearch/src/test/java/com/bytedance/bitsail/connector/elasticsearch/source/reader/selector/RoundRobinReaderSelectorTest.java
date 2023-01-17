/*
 * Copyright 2022 Bytedance Ltd. and/or its affiliates.
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

package com.bytedance.bitsail.connector.elasticsearch.source.reader.selector;

import com.google.common.collect.Maps;
import org.apache.commons.compress.utils.Lists;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class RoundRobinReaderSelectorTest {

  private ReaderSelector selector;

  private final int readerNumber = 3;

  private final int splitNumber = 10;

  @Before
  public void setup() {
    selector = new RoundRobinReaderSelector(readerNumber);
  }

  @Test
  public void testGetReaderIndex() {
    Map<Integer, List<Integer>> map = Maps.newHashMap();
    for (int i = 0; i < splitNumber; i++) {
      int idx = selector.getReaderIndex("");
      List<Integer> list = map.getOrDefault(idx, Lists.newArrayList());
      list.add(i);
      map.put(idx, list);
    }
    assertEquals(4, map.get(0).size());
    assertEquals(3, map.get(1).size());
    assertEquals(3, map.get(2).size());
  }
}
