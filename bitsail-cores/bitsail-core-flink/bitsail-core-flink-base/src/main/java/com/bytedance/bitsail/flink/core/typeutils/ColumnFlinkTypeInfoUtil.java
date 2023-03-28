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

package com.bytedance.bitsail.flink.core.typeutils;

import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.exception.CommonErrorCode;
import com.bytedance.bitsail.common.model.ColumnInfo;
import com.bytedance.bitsail.common.type.TypeInfoConverter;
import com.bytedance.bitsail.common.typeinfo.TypeInfo;
import com.bytedance.bitsail.flink.core.typeinfo.ListColumnTypeInfo;
import com.bytedance.bitsail.flink.core.typeinfo.MapColumnTypeInfo;

import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;

/**
 * flink type information helper
 *
 * @desc: type system upgrade, after Plugin improvement, FlinkTypeUtil will be deprecated, please use {@link ColumnFlinkTypeInfoUtil}
 */

public class ColumnFlinkTypeInfoUtil {
  private static final Logger LOG = LoggerFactory.getLogger(ColumnFlinkTypeInfoUtil.class);

  public static RowTypeInfo getRowTypeInformation(TypeInfoConverter converter,
                                                  List<ColumnInfo> columnInfos) {

    LOG.debug("TypeInfoConverter = {}.", converter);
    String[] fieldNames = new String[columnInfos.size()];
    TypeInformation<?>[] fieldTypes = new TypeInformation[columnInfos.size()];
    for (int index = 0; index < columnInfos.size(); index++) {
      String type = StringUtils.lowerCase(columnInfos.get(index).getType());
      String name = columnInfos.get(index).getName();

      TypeInfo<?> typeInfo = converter.fromTypeString(type);
      if (Objects.isNull(typeInfo)) {
        throw BitSailException.asBitSailException(CommonErrorCode.UNSUPPORTED_COLUMN_TYPE,
            String.format("Column %s type string %s is not support.", name, type));
      }
      fieldNames[index] = name;
      fieldTypes[index] = toColumnFlinkTypeInformation(typeInfo);
    }

    return new RowTypeInfo(fieldTypes, fieldNames);
  }

  public static RowTypeInfo getRowTypeInformation(com.bytedance.bitsail.common.typeinfo.RowTypeInfo rowTypeInfo) {

    TypeInformation<?>[] fieldTypes = new TypeInformation[rowTypeInfo.getTypeInfos().length];
    for (int index = 0; index < rowTypeInfo.getTypeInfos().length; index++) {
      fieldTypes[index] = toColumnFlinkTypeInformation(rowTypeInfo.getTypeInfos()[index]);
    }
    return new RowTypeInfo(fieldTypes, rowTypeInfo.getFieldNames());
  }

  public static com.bytedance.bitsail.common.typeinfo.RowTypeInfo getRowTypeInfo(RowTypeInfo rowTypeInfo) {

    TypeInfo<?>[] fieldTypes = new TypeInfo[rowTypeInfo.getFieldTypes().length];
    for (int index = 0; index < rowTypeInfo.getFieldTypes().length; index++) {
      fieldTypes[index] = toTypeInfo(rowTypeInfo.getFieldTypes()[index]);
    }
    return new com.bytedance.bitsail.common.typeinfo.RowTypeInfo(rowTypeInfo.getFieldNames(), fieldTypes);
  }

  private static TypeInformation<?> toColumnFlinkTypeInformation(TypeInfo<?> typeInfo) {
    if (typeInfo instanceof com.bytedance.bitsail.common.typeinfo.MapTypeInfo) {
      com.bytedance.bitsail.common.typeinfo.MapTypeInfo<?, ?> mapTypeInfo = (com.bytedance.bitsail.common.typeinfo.MapTypeInfo<?, ?>) typeInfo;
      TypeInfo<?> keyTypeInfo = mapTypeInfo.getKeyTypeInfo();
      TypeInfo<?> valueTypeInfo = mapTypeInfo.getValueTypeInfo();
      return new MapColumnTypeInfo(toColumnFlinkTypeInformation(keyTypeInfo),
          toColumnFlinkTypeInformation(valueTypeInfo));
    }

    if (typeInfo instanceof com.bytedance.bitsail.common.typeinfo.ListTypeInfo) {
      com.bytedance.bitsail.common.typeinfo.ListTypeInfo<?> listTypeInfo = (com.bytedance.bitsail.common.typeinfo.ListTypeInfo<?>) typeInfo;
      TypeInfo<?> elementTypeInfo = listTypeInfo.getElementTypeInfo();
      return new ListColumnTypeInfo(toColumnFlinkTypeInformation(elementTypeInfo));
    }

    return TypeInfoColumnBridge.bridgeTypeInfo(typeInfo);
  }

  public static TypeInfo<?> toTypeInfo(TypeInformation<?> typeInformation) {
    if (typeInformation instanceof MapColumnTypeInfo) {
      MapColumnTypeInfo<?, ?> mapTypeInfo = (MapColumnTypeInfo<?, ?>) typeInformation;
      TypeInformation<?> keyTypeInfo = mapTypeInfo.getKeyTypeInfo();
      TypeInformation<?> valueTypeInfo = mapTypeInfo.getValueTypeInfo();
      return new com.bytedance.bitsail.common.typeinfo.MapTypeInfo<>(toTypeInfo(keyTypeInfo),
          toTypeInfo(valueTypeInfo));
    }

    if (typeInformation instanceof ListColumnTypeInfo) {
      ListColumnTypeInfo<?> listTypeInfo = (ListColumnTypeInfo<?>) typeInformation;
      return new com.bytedance.bitsail.common.typeinfo.ListTypeInfo<>(toTypeInfo(listTypeInfo.getElementTypeInfo()));
    }

    return TypeInfoColumnBridge.bridgeTypeInformation(typeInformation);
  }
}
