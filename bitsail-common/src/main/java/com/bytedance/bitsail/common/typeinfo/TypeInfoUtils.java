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

package com.bytedance.bitsail.common.typeinfo;

import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.exception.CommonErrorCode;
import com.bytedance.bitsail.common.model.ColumnInfo;
import com.bytedance.bitsail.common.type.BitSailTypeParser;
import com.bytedance.bitsail.common.type.TypeInfoConverter;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class TypeInfoUtils {

  public static TypeInfo<?>[] getTypeInfos(TypeInfoConverter converter,
                                           List<ColumnInfo> columnInfos) {

    TypeInfo<?>[] fieldTypes = new TypeInfo[columnInfos.size()];
    for (int index = 0; index < columnInfos.size(); index++) {
      String type = StringUtils.lowerCase(columnInfos.get(index).getType());
      TypeInfo<?> typeInfo = converter.fromTypeString(type);
      if (Objects.isNull(typeInfo)) {
        throw BitSailException.asBitSailException(CommonErrorCode.CONVERT_NOT_SUPPORT,
            String.format("Type converter = %s not support type %s.", converter, type));
      }
      List<TypeProperty> typeProperties = BitSailTypeParser
          .fromTypePropertyString(columnInfos.get(index).getProperties());

      //1. type property should not share in same type definition, so we need create a new one.
      //2. type property only support in basic type info.
      if (typeInfo instanceof BasicTypeInfo && CollectionUtils.isNotEmpty(typeProperties)) {
        typeInfo = new BasicTypeInfo<>(typeInfo.getTypeClass(), typeProperties);
      }
      fieldTypes[index] = typeInfo;
    }
    return fieldTypes;
  }

  public static RowTypeInfo getRowTypeInfo(TypeInfoConverter converter, List<ColumnInfo> columnInfos) {
    if (Objects.isNull(columnInfos)) {
      throw BitSailException.asBitSailException(CommonErrorCode.CONFIG_ERROR, "The columns option should not be null.");
    }
    String[] fieldNames = columnInfos
        .stream()
        .map(ColumnInfo::getName)
        .collect(Collectors.toList()).toArray(new String[] {});

    return new RowTypeInfo(fieldNames, getTypeInfos(converter, columnInfos));
  }
}
