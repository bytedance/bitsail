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

package com.bytedance.bitsail.connector.larksheet.util;

import java.io.Serializable;

/**
 * Some features when transforming data.<br/>
 * Ref: <a href="https://open.feishu.cn/document/ukTMukTMukTM/ugTMzUjL4EzM14COxMTN">Read a single range</a>
 */
public enum Feature implements Serializable {

  FORMULA_VALUE_RENDER("valueRenderOption", "ToString"),
  DATE_TIME_RENDER("dateTimeRenderOption", "FormattedString");

  final String key;
  final String value;

  Feature(String defaultKey, String defaultValue) {
    this.key = defaultKey;
    this.value = defaultValue;
  }
}
