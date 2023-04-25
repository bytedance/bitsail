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

import com.bytedance.bitsail.base.connector.transform.v1.BitSailMapFunction;
import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.column.StringColumn;
import com.bytedance.bitsail.common.exception.CommonErrorCode;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.common.typeinfo.RowTypeInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfos;

import java.util.List;

public class AppendStringMapFunction<I extends Object, O extends Object> implements BitSailMapFunction<I, O> {

  private final List<Integer> position;
  private final List<String> appendList;

  private final RowTypeInfo inputType;

  public AppendStringMapFunction(List<Integer> position, List<String> appendList, RowTypeInfo inputType) {
    this.position = position;
    this.appendList = appendList;
    this.inputType = inputType;
    for (int index : position) {
      if (inputType.getTypeInfos()[index] != TypeInfos.STRING_TYPE_INFO) {
        throw BitSailException.asBitSailException(
            CommonErrorCode.UNSUPPORTED_COLUMN_TYPE, "Only string type field can use AppendStringMapFunction");
      }
    }
  }

  @Override
  public RowTypeInfo getOutputType() {
    // this function doesn't change type
    return inputType;
  }

  @Override
  public Object map(Object input) throws Exception {
    if (input instanceof Row) {
      return handleRow((Row) input);
    } else if (input instanceof org.apache.flink.types.Row) {
      return handleFlinkRow((org.apache.flink.types.Row) input);
    } else {
      throw BitSailException.asBitSailException(
          CommonErrorCode.RUNTIME_ERROR, "map function get unexpected type, only BitSail row type and Flink row type are supported");
    }
  }

  private Row handleRow(Row input) {
    for (int i = 0; i < position.size(); i++) {
      int curIndex = position.get(i);
      String appendVal = appendList.get(i);
      input.setField(curIndex, input.getString(curIndex).concat(appendVal));
    }
    return input;
  }

  private org.apache.flink.types.Row handleFlinkRow(org.apache.flink.types.Row input) {
    for (int i = 0; i < position.size(); i++) {
      int curIndex = position.get(i);
      String appendVal = appendList.get(i);
      String origin = ((StringColumn) input.getField(curIndex)).asString();
      input.setField(curIndex, new StringColumn(origin.concat(appendVal)));
    }
    return input;
  }
}
