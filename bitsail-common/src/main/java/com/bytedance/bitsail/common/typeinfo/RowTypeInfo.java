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

import com.bytedance.bitsail.common.row.Row;

import java.util.Arrays;

public class RowTypeInfo extends TypeInfo<Row> {

  private final String[] fieldNames;

  private final TypeInfo<?>[] typeInfos;

  public RowTypeInfo(String[] fieldNames, TypeInfo<?>[] typeInfos) {
    this.fieldNames = fieldNames;
    this.typeInfos = typeInfos;
  }

  @Override
  public String toString() {
    StringBuilder bld = new StringBuilder("Row");
    if (typeInfos.length > 0) {
      bld.append('(').append(fieldNames[0]).append(": ").append(typeInfos[0]);

      for (int i = 1; i < typeInfos.length; i++) {
        bld.append(", ").append(fieldNames[i]).append(": ").append(typeInfos[i]);
      }

      bld.append(')');
    }
    return bld.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RowTypeInfo that = (RowTypeInfo) o;
    return Arrays.equals(fieldNames, that.fieldNames) && Arrays.equals(typeInfos, that.typeInfos);
  }

  public int indexOf(String fieldName) {
    for (int index = 0; index < fieldNames.length; index++) {
      if (fieldNames[index].equals(fieldName)) {
        return index;
      }
    }
    return -1;
  }

  @Override
  public int hashCode() {
    int result = Arrays.hashCode(fieldNames);
    result = 31 * result + Arrays.hashCode(typeInfos);
    return result;
  }

  public String[] getFieldNames() {
    return fieldNames;
  }

  public TypeInfo<?>[] getTypeInfos() {
    return typeInfos;
  }

  @Override
  public Class<Row> getTypeClass() {
    return (Class<Row>) Row.class;
  }

}
