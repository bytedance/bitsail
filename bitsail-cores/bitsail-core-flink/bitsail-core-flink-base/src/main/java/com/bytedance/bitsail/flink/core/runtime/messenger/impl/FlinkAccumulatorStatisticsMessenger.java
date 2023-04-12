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

package com.bytedance.bitsail.flink.core.runtime.messenger.impl;

import com.bytedance.bitsail.base.execution.Mode;
import com.bytedance.bitsail.base.messenger.BaseStatisticsMessenger;
import com.bytedance.bitsail.base.messenger.common.MessengerCounterType;
import com.bytedance.bitsail.base.messenger.common.MessengerGroup;
import com.bytedance.bitsail.base.messenger.context.MessengerContext;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.option.CommonOptions;
import com.bytedance.bitsail.flink.core.runtime.RuntimeContextInjectable;
import com.bytedance.bitsail.flink.core.runtime.messenger.DropwizardCounterWrapper;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;

public class FlinkAccumulatorStatisticsMessenger extends BaseStatisticsMessenger implements RuntimeContextInjectable {

  protected static final String GROUP_BITSAIL = "bitsail";
  protected static final String SUCCESS_RECORDS_RATE = "succ_records_rate";
  protected static final String FAILED_RECORDS_RATE = "failed_records_rate";
  protected static final String SUCCESS_BYTES_RATE = "succ_bytes_rate";
  protected static final String SUCCESS_RECORDS = "succ_records";
  protected static final String FAILED_RECORDS = "failed_records";
  protected static final String SUCCESS_BYTES = "succ_bytes";
  protected static final String INPUT_SPLITS = "splits";

  private transient RuntimeContext runtimeContext;
  private transient volatile boolean committed;
  private transient Mode mode;

  public FlinkAccumulatorStatisticsMessenger(MessengerContext messengerContext,
                                             BitSailConfiguration commonConfiguration) {
    super(messengerContext);
    this.mode = Mode.getJobRunMode(commonConfiguration.get(CommonOptions.JOB_TYPE));
  }

  @Override
  public void open() {
    super.open();
    if (Mode.BATCH.equals(mode)) {
      registerMetricGroup();
    }
  }

  private void registerMetricGroup() {
    String group = StringUtils.lowerCase(messengerContext.getMessengerGroup().getName());
    runtimeContext.getMetricGroup()
        .addGroup(GROUP_BITSAIL)
        .addGroup(group)
        .counter(SUCCESS_RECORDS, new DropwizardCounterWrapper(coreSuccessCounter));
    runtimeContext.getMetricGroup()
        .addGroup(GROUP_BITSAIL)
        .addGroup(group)
        .counter(FAILED_RECORDS, new DropwizardCounterWrapper(coreFailedCounter));
    runtimeContext.getMetricGroup()
        .addGroup(GROUP_BITSAIL)
        .addGroup(group)
        .counter(SUCCESS_BYTES, new DropwizardCounterWrapper(coreSuccessByteCounter));

    runtimeContext.getMetricGroup()
        .addGroup(GROUP_BITSAIL)
        .addGroup(group)
        .meter(SUCCESS_RECORDS_RATE, new DropwizardMeterWrapper(coreSuccessRecordsMeter));

    runtimeContext.getMetricGroup()
        .addGroup(GROUP_BITSAIL)
        .addGroup(group)
        .meter(FAILED_RECORDS_RATE, new DropwizardMeterWrapper(coreFailedRecordsMeter));

    runtimeContext.getMetricGroup()
        .addGroup(GROUP_BITSAIL)
        .addGroup(group)
        .meter(SUCCESS_BYTES_RATE, new DropwizardMeterWrapper(coreSuccessByteMeter));

    if (MessengerGroup.READER.equals(messengerContext.getMessengerGroup())) {
      runtimeContext.getMetricGroup()
          .addGroup(GROUP_BITSAIL)
          .addGroup(group)
          .counter(INPUT_SPLITS, new DropwizardCounterWrapper(finishedSplitCounter));
    }
  }

  @Override
  public void setRuntimeContext(RuntimeContext runtimeContext) {
    this.runtimeContext = runtimeContext;
  }

  @Override
  public void commit() {
    if (committed || Mode.STREAMING.equals(mode)) {
      return;
    }
    doCommitAction();
    committed = true;
  }

  private void doCommitAction() {
    runtimeContext.getLongCounter(
            messengerContext.getCompleteCounterName(MessengerCounterType.SUCCESS_RECORDS_COUNT))
        .add(getSuccessRecords());

    runtimeContext.getLongCounter(
            messengerContext.getCompleteCounterName(MessengerCounterType.SUCCESS_RECORDS_BYTES))
        .add(getSuccessRecordBytes());

    runtimeContext.getLongCounter(
            messengerContext.getCompleteCounterName(MessengerCounterType.FAILED_RECORDS_COUNT))
        .add(getFailedRecords());

    runtimeContext.getLongCounter(
            messengerContext.getCompleteCounterName(MessengerCounterType.TASK_RETRY_COUNT))
        .add(runtimeContext.getAttemptNumber());
  }
}
