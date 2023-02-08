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

package com.bytedance.bitsail.common.util;

import org.apache.commons.lang3.StringUtils;

/**
 * Created 2020/12/23.
 */
public class LogUtils {

  public static String logCut(String log,
                              int length) {
    if (StringUtils.length(log) <= length) {
      return log;
    }
    return StringUtils.substring(log, 0, length);
  }
}
