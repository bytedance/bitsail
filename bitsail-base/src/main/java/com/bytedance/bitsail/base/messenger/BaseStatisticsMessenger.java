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

package com.bytedance.bitsail.base.messenger;

import com.bytedance.bitsail.base.messenger.context.MessengerContext;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;

import java.io.IOException;

public class BaseStatisticsMessenger implements Messenger {

  protected final MessengerContext messengerContext;

  protected transient Counter coreSuccessCounter;
  protected transient Counter coreFailedCounter;
  protected transient Counter coreSuccessByteCounter;
  protected transient Counter finishedSplitCounter;
  protected transient Meter coreSuccessRecordsMeter;
  protected transient Meter coreSuccessByteMeter;
  protected transient Meter coreFailedRecordsMeter;

  public BaseStatisticsMessenger(MessengerContext messengerContext) {
    this.messengerContext = messengerContext;
  }

  @Override
  public void open() {
    prepareMessenger();
  }

  private void prepareMessenger() {
    coreSuccessCounter = new Counter();
    coreFailedCounter = new Counter();
    coreSuccessByteCounter = new Counter();
    coreSuccessRecordsMeter = new Meter();
    coreSuccessByteMeter = new Meter();
    coreFailedRecordsMeter = new Meter();

    finishedSplitCounter = new Counter();
  }

  @Override
  public void addSuccessRecord(long byteSize) {
    coreSuccessCounter.inc();
    coreSuccessByteCounter.inc(byteSize);
    coreSuccessRecordsMeter.mark();
    coreSuccessByteMeter.mark(byteSize);
  }

  @Override
  public void addFailedRecord(Throwable throwable) {
    coreFailedCounter.inc();
    coreFailedRecordsMeter.mark();
  }

  @Override
  public void recordSplitProgress() {
    finishedSplitCounter.inc();
  }

  @Override
  public long getSuccessRecords() {
    return coreSuccessCounter.getCount();
  }

  @Override
  public long getSuccessRecordBytes() {
    return coreSuccessByteCounter.getCount();
  }

  @Override
  public long getFailedRecords() {
    return coreFailedCounter.getCount();
  }

  @Override
  public void close() throws IOException {
    //ignore
  }
}
