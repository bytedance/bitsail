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

package com.bytedance.bitsail.base.ratelimit;

/**
 * @desc:
 */
public class CommunicationTool {

  public static final String SUCCEED_RECORDS = "succeedRecords";
  public static final String SUCCEED_BYTES = "succeedBytes";

  public static final String FAILED_RECORDS = "readFailedRecords";
  public static final String FAILED_BYTES = "readFailedBytes";

  public static long getTotalRecords(final Communication communication) {
    return communication.getCounterVal(SUCCEED_RECORDS) +
        communication.getCounterVal(FAILED_RECORDS);
  }

  public static long getTotalBytes(final Communication communication) {
    return communication.getCounterVal(SUCCEED_BYTES) +
        communication.getCounterVal(FAILED_BYTES);
  }
}
