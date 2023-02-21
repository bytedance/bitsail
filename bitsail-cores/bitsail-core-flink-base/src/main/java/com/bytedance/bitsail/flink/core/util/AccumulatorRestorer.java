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

package com.bytedance.bitsail.flink.core.util;

import com.bytedance.bitsail.base.execution.ProcessResult;
import com.bytedance.bitsail.base.messenger.common.MessengerCounterType;
import com.bytedance.bitsail.base.messenger.common.MessengerGroup;
import com.bytedance.bitsail.base.messenger.context.MessengerContext;
import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.exception.CommonErrorCode;

import org.apache.flink.api.common.JobExecutionResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;

public class AccumulatorRestorer {

  private static final Logger LOG = LoggerFactory.getLogger(AccumulatorRestorer.class);

  public static void restoreAccumulator(ProcessResult<JobExecutionResult> processResult,
                                        MessengerContext messengerContext) {
    if (Objects.isNull(processResult) || Objects.isNull(processResult.getJobExecutionResult())) {
      return;
    }
    JobExecutionResult jobExecutionResult = processResult.getJobExecutionResult();
    Map<String, Object> accumulatorResults = jobExecutionResult.getAllAccumulatorResults();
    MessengerGroup messengerGroup = messengerContext.getMessengerGroup();
    switch (messengerGroup) {
      case READER:
        restoreReaderAccumulator(messengerContext, accumulatorResults, processResult);
        break;
      case WRITER:
        restoreWriterAccumulator(messengerContext, accumulatorResults, processResult);
        break;
      default:
        throw BitSailException.asBitSailException(CommonErrorCode.RUNTIME_ERROR, "");
    }
  }

  private static void restoreReaderAccumulator(MessengerContext messengerContext,
                                               Map<String, Object> allAccumulatorResults,
                                               ProcessResult<?> processResult) {
    processResult.setJobFailedInputRecordCount(getAccumulatorValue(messengerContext.getCompleteCounterName(MessengerCounterType.FAILED_RECORDS_COUNT),
        allAccumulatorResults));
    processResult.setJobSuccessInputRecordCount(getAccumulatorValue(messengerContext.getCompleteCounterName(MessengerCounterType.SUCCESS_RECORDS_COUNT),
        allAccumulatorResults));
    processResult.setJobSuccessInputRecordBytes(getAccumulatorValue(messengerContext.getCompleteCounterName(MessengerCounterType.SUCCESS_RECORDS_BYTES),
        allAccumulatorResults));
  }

  private static void restoreWriterAccumulator(MessengerContext messengerContext,
                                               Map<String, Object> accumulatorResults,
                                               ProcessResult<JobExecutionResult> processResult) {
    processResult.setJobFailedOutputRecordCount(getAccumulatorValue(messengerContext.getCompleteCounterName(MessengerCounterType.FAILED_RECORDS_COUNT),
        accumulatorResults));
    processResult.setJobSuccessOutputRecordCount(getAccumulatorValue(messengerContext.getCompleteCounterName(MessengerCounterType.SUCCESS_RECORDS_COUNT),
        accumulatorResults));
    processResult.setJobSuccessOutputRecordBytes(getAccumulatorValue(messengerContext.getCompleteCounterName(MessengerCounterType.SUCCESS_RECORDS_BYTES),
        accumulatorResults));
  }

  private static long getAccumulatorValue(String name, Map<String, Object> allAccumulatorResults) {
    return (long) allAccumulatorResults.getOrDefault(name, 0L);
  }
}
