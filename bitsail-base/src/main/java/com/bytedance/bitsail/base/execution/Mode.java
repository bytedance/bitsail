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

package com.bytedance.bitsail.base.execution;

import org.apache.commons.lang3.StringUtils;

/**
 * Created 2022/4/21
 */
public enum Mode {

  STREAMING,

  BATCH;

  public static Mode getJobRunMode(String jobType) {
    for (Mode mode : Mode.values()) {
      if (StringUtils.equalsIgnoreCase(jobType.trim(), mode.name())) {
        return mode;
      }
    }
    throw new UnsupportedOperationException(String.format("Unsupported job type: %s", jobType));
  }
}
