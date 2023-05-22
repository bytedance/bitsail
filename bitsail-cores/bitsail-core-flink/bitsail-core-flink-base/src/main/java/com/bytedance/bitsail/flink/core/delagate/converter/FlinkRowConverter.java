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

package com.bytedance.bitsail.flink.core.delagate.converter;

import com.bytedance.bitsail.base.serializer.RowConverter;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.typeinfo.RowTypeInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfoValueConverter;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created 2022/6/21
 */
public class FlinkRowConverter implements RowConverter<Row> {

  private static final Logger LOG = LoggerFactory.getLogger(FlinkRowConverter.class);

  private final RowTypeInfo rowTypeInfo;

  private final TypeInfoValueConverter typeInfoValueConverter;

  public FlinkRowConverter(RowTypeInfo rowTypeInfo,
                           BitSailConfiguration commonConfiguration) {
    this.rowTypeInfo = rowTypeInfo;
    this.typeInfoValueConverter = new TypeInfoValueConverter(commonConfiguration);
  }

  /**
   * Commonly run in source side, will transform bitsail row to flink row.
   */
  @Override
  public Row to(com.bytedance.bitsail.common.row.Row row) throws IOException {
    Object[] fields = row.getFields();
    int arity = ArrayUtils.getLength(fields);
    Row flinkRow = new Row(org.apache.flink.types.RowKind.fromByteValue(row.getKind().toByteValue()), arity);
    for (int index = 0; index < arity; index++) {
      TypeInfo<?> typeInfo = rowTypeInfo.getTypeInfos()[index];
      Object value = row.getField(index);
      String name = rowTypeInfo.getFieldNames()[index];
      value = wrapperValueConverter(name, value, typeInfo);
      flinkRow.setField(index, value);
    }
    return flinkRow;
  }

  private Object wrapperValueConverter(String name,
                                       Object value,
                                       TypeInfo<?> typeInfo) {
    try {
      return typeInfoValueConverter.convertObject(value, typeInfo);
    } catch (Exception e) {
      LOG.error("Convert column name: {}'s value: {} to type info's definition {} failed.", name, value, typeInfo.getTypeClass());
      throw e;
    }
  }

  /**
   * Commonly run in sink side, we try to transform flink row to bitsail row.
   */
  @Override
  public com.bytedance.bitsail.common.row.Row from(Row serialized) throws IOException {
    int arity = serialized.getArity();
    Object[] fields = new Object[arity];
    for (int index = 0; index < arity; index++) {
      TypeInfo<?> typeInfo = rowTypeInfo.getTypeInfos()[index];
      Object value = serialized.getField(index);
      String name = rowTypeInfo.getFieldNames()[index];
      fields[index] = wrapperValueConverter(name, value, typeInfo);
    }
    return new com.bytedance.bitsail.common.row.Row(
        serialized.getKind().toByteValue(),
        fields);
  }
}
