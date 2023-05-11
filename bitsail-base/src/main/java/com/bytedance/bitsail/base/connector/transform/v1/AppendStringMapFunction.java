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

package com.bytedance.bitsail.base.connector.transform.v1;

import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.exception.CommonErrorCode;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.common.typeinfo.RowTypeInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfos;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class AppendStringMapFunction<I extends Row, O extends Row> implements BitSailMapFunction<I, O> {

  private List<Integer> position;
  private final List<String> appendList;

  private final RowTypeInfo inputType;

  public AppendStringMapFunction(List<String> selectedColumns, List<String> appendList, RowTypeInfo inputType) {
    this.position = getPosition(selectedColumns, Arrays.stream(inputType.getFieldNames()).collect(Collectors.toList()));
    this.appendList = appendList;
    this.inputType = inputType;
    for (int index : position) {
      if (inputType.getTypeInfos()[index] != TypeInfos.STRING_TYPE_INFO) {
        throw BitSailException.asBitSailException(
            CommonErrorCode.UNSUPPORTED_COLUMN_TYPE, String.format(
                "Only string type field can be used in AppendStringMapFunction, " +
                "but found %s at position %s", inputType.getTypeInfos()[index].toString(), index));
      }
    }
  }

  @Override
  public Row map(Row input) throws Exception {
    for (int i = 0; i < position.size(); i++) {
      int curIndex = position.get(i);
      String appendVal = appendList.get(i);
      input.setField(curIndex, input.getString(curIndex).concat(appendVal));
    }
    return input;
  }

  @Override
  public RowTypeInfo getOutputType() {
    // this function doesn't change type
    return inputType;
  }

  private static List<Integer> getPosition(List<String> selectedColumns, List<String> fullColumns) {
    if (selectedColumns.isEmpty()) {
      throw BitSailException.asBitSailException(CommonErrorCode.TRANSFORM_ERROR,
          "APPEND_STRING_COLUMNS could not be an empty list.");
    }
    List<Integer> indexes = new ArrayList<>(selectedColumns.size());
    for (String col : selectedColumns) {
      int index = fullColumns.indexOf(col);
      if (index == -1) {
        throw BitSailException.asBitSailException(CommonErrorCode.TRANSFORM_ERROR,
            String.format("APPEND_STRING_COLUMNS %s not found in the columns of the source %s.", col, fullColumns));
      }
      indexes.add(index);
    }
    return indexes;
  }
}
