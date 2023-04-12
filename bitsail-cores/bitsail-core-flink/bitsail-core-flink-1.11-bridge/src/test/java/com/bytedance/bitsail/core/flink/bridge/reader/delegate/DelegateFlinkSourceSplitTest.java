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

package com.bytedance.bitsail.core.flink.bridge.reader.delegate;

import com.bytedance.bitsail.base.connector.reader.v1.SourceEvent;
import com.bytedance.bitsail.core.flink.bridge.reader.builder.MockSource;

import com.google.common.collect.ImmutableList;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class DelegateFlinkSourceSplitTest {

  MockSource source;
  SplitEnumeratorContext<DelegateFlinkSourceSplit<MockSource.MockSourceSplit>> splitEnumerateContext;

  @Before
  public void init() {
    source = new MockSource();
    splitEnumerateContext = (SplitEnumeratorContext<DelegateFlinkSourceSplit<MockSource.MockSourceSplit>>)
        Mockito.mock(SplitEnumeratorContext.class);
  }

  @Test
  public void testEnumerator() {
    Throwable caught = null;

    try {
      Tuple2<Long, String> checkpoint = Tuple2.of(1L, "cp-1");
      DelegateFlinkSourceSplitEnumerator<MockSource.MockSourceSplit, String> enumerator =
          new DelegateFlinkSourceSplitEnumerator<>(
              source::createSplitCoordinator,
              splitEnumerateContext,
              checkpoint
          );
      enumerator.start();
      enumerator.handleSplitRequest(0, "127.0.0.1");
      enumerator.addSplitsBack(ImmutableList.of(
          new DelegateFlinkSourceSplit<>(new MockSource.MockSourceSplit())
      ), 0);
      enumerator.addReader(0);
      enumerator.handleSourceEvent(0, new DelegateSourceEvent(Mockito.mock(SourceEvent.class)));
      enumerator.snapshotState();
      enumerator.notifyCheckpointComplete(1);
      enumerator.close();
    } catch (Throwable t) {
      caught = t;
    }
    Assert.assertNull(caught);
  }
}
