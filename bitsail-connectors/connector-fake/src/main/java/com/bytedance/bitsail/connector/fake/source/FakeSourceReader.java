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

package com.bytedance.bitsail.connector.fake.source;

import com.bytedance.bitsail.base.connector.reader.v1.SourcePipeline;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.common.typeinfo.RowTypeInfo;
import com.bytedance.bitsail.connector.base.source.SimpleSourceReaderBase;
import com.bytedance.bitsail.connector.fake.option.FakeReaderOptions;

import com.google.common.util.concurrent.RateLimiter;

import java.util.concurrent.atomic.AtomicLong;

public class FakeSourceReader extends SimpleSourceReaderBase<Row> {

  private final BitSailConfiguration readerConfiguration;
  private final RowTypeInfo rowTypeInfo;

  private final transient int totalCount;
  private final transient RateLimiter fakeGenerateRate;
  private final transient AtomicLong counter;

  private final FakeRowGenerator fakeRowGenerator;

  public FakeSourceReader(BitSailConfiguration readerConfiguration, Context context) {
    this.readerConfiguration = readerConfiguration;
    this.rowTypeInfo = context.getRowTypeInfo();
    this.totalCount = readerConfiguration.get(FakeReaderOptions.TOTAL_COUNT);
    this.fakeGenerateRate = RateLimiter.create(readerConfiguration.get(FakeReaderOptions.RATE));
    this.counter = new AtomicLong();
    this.fakeRowGenerator = new FakeRowGenerator(readerConfiguration, context.getIndexOfSubtask());
  }

  @Override
  public void pollNext(SourcePipeline<Row> pipeline) throws Exception {
    fakeGenerateRate.acquire();
    pipeline.output(fakeRowGenerator.fakeOneRecord(rowTypeInfo.getTypeInfos()));
  }

  @Override
  public boolean hasMoreElements() {
    return counter.incrementAndGet() <= totalCount;
  }
}
