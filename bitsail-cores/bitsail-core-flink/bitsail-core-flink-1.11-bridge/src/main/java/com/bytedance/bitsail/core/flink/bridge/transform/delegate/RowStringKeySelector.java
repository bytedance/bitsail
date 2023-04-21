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

package com.bytedance.bitsail.core.flink.bridge.transform.delegate;

import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.exception.CommonErrorCode;
import com.bytedance.bitsail.common.row.Row;

import org.apache.flink.api.java.functions.KeySelector;

public class RowStringKeySelector<INPUT extends Object, KEY extends Object> implements KeySelector<INPUT, KEY> {

  private final int keyIndex;

  public static int DEFAULT_KEY_INDEX = 0;

  public RowStringKeySelector(int keyIndex) {
    this.keyIndex = keyIndex;
  }

  @Override
  public Object getKey(Object value) throws Exception {
    if (value instanceof Row) {
      return ((Row) value).getField(keyIndex);
    } else if (value instanceof org.apache.flink.types.Row) {
      return ((org.apache.flink.types.Row) value).getField(keyIndex);
    } else {
      throw BitSailException.asBitSailException(
          CommonErrorCode.RUNTIME_ERROR, "partitioner get unexpected type, only BitSail row type and Flink row type are supported");
    }
  }
}
