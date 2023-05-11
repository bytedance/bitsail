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
import com.bytedance.bitsail.common.typeinfo.RowTypeInfo;

import org.apache.flink.api.java.functions.KeySelector;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class RowKeySelector<INPUT extends Object, KEY extends Object> implements KeySelector<INPUT, KEY> {

  private int keyIndex;

  public RowKeySelector(RowTypeInfo rowTypeInfo, String keyColumn) {
    List<String> fullColumns = Arrays.stream(rowTypeInfo.getFieldNames()).collect(Collectors.toList());
    keyIndex = fullColumns.indexOf(keyColumn);
    if (keyIndex == -1) {
      throw BitSailException.asBitSailException(CommonErrorCode.TRANSFORM_ERROR,
          String.format("partition_column_name %s not found in the columns of the source %s.", keyColumn, fullColumns));
    }
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
