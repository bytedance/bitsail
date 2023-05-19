/*
 * Copyright 2022-2023 Bytedance Ltd. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.bytedance.bitsail.transforms.map.normalization;

import com.bytedance.bitsail.common.option.ConfigOption;
import com.bytedance.bitsail.common.option.TransformOptions;

import com.alibaba.fastjson.TypeReference;

import java.util.List;

import static com.bytedance.bitsail.common.option.ConfigOptions.key;

public interface NormalizationOptions extends TransformOptions {

  ConfigOption<List<String>> APPEND_STRING_COLUMNS =
      key("append_string_columns")
          .onlyReference(new TypeReference<List<String>>() {
          });

  ConfigOption<List<String>> APPEND_STRING_VALUES =
      key("append_string_values")
          .onlyReference(new TypeReference<List<String>>() {
          });
}
