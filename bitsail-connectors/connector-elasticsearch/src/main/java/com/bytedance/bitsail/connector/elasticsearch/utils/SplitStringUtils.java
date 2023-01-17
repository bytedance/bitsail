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

package com.bytedance.bitsail.connector.elasticsearch.utils;

import java.util.ArrayList;
import java.util.List;

import static com.bytedance.bitsail.connector.elasticsearch.base.EsConstants.SPLIT_COMMA;

public class SplitStringUtils {

  public static String[] splitString(String indices) {
    String[] splits = indices.split(SPLIT_COMMA);
    List<String> validNames = new ArrayList<>();
    for (String name : splits) {
      if (name.trim().length() > 0) {
        validNames.add(name.trim());
      }
    }
    return validNames.toArray(new String[0]);
  }
}
