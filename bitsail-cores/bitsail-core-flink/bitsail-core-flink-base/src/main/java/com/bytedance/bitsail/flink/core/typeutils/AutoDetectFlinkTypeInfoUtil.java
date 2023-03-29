/*
 *       Copyright 2022-2023 Bytedance Ltd. and/or its affiliates.
 *
 *       Licensed under the Apache License, Version 2.0 (the "License");
 *       you may not use this file except in compliance with the License.
 *       You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *       Unless required by applicable law or agreed to in writing, software
 *       distributed under the License is distributed on an "AS IS" BASIS,
 *       WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *       See the License for the specific language governing permissions and
 *       limitations under the License.
 */

package com.bytedance.bitsail.flink.core.typeutils;

import com.bytedance.bitsail.common.typeinfo.RowTypeInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfo;
import com.bytedance.bitsail.flink.core.typeinfo.ListColumnTypeInfo;
import com.bytedance.bitsail.flink.core.typeinfo.MapColumnTypeInfo;
import com.bytedance.bitsail.flink.core.typeinfo.PrimitiveColumnTypeInfo;

import org.apache.flink.api.common.typeinfo.TypeInformation;

/**
 * Auto-detect flink type info belong to column type info or native type info.
 */
public class AutoDetectFlinkTypeInfoUtil {

  public static RowTypeInfo bridgeRowTypeInfo(org.apache.flink.api.java.typeutils.RowTypeInfo rowTypeInfo) {
    TypeInformation<?>[] fieldTypes = rowTypeInfo.getFieldTypes();

    TypeInfo<?>[] bridged = new TypeInfo<?>[fieldTypes.length];

    for (int index = 0; index < rowTypeInfo.getFieldTypes().length; index++) {
      TypeInformation<?> fieldType = rowTypeInfo.getFieldTypes()[index];

      if (fieldType instanceof PrimitiveColumnTypeInfo
          || fieldType instanceof ListColumnTypeInfo
          || fieldType instanceof MapColumnTypeInfo) {
        bridged[index] = ColumnFlinkTypeInfoUtil.toTypeInfo(fieldType);
      } else {
        bridged[index] = NativeFlinkTypeInfoUtil.toTypeInfo(fieldType);
      }
    }
    return new com.bytedance.bitsail.common.typeinfo.RowTypeInfo(rowTypeInfo.getFieldNames(), bridged);
  }
}
