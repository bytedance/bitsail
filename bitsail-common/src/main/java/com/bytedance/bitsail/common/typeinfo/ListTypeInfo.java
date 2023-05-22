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

import com.bytedance.bitsail.common.util.JsonSerializer;
import com.bytedance.bitsail.common.util.Preconditions;

import java.util.List;

public class ListTypeInfo<T> extends TypeInfo<List<T>> {

  private final TypeInfo<T> elementTypeInfo;

  public ListTypeInfo(TypeInfo<T> elementTypeInfo) {
    Preconditions.checkNotNull(elementTypeInfo, "elementTypeInfo can not be null");
    this.elementTypeInfo = elementTypeInfo;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Class<List<T>> getTypeClass() {
    return (Class<List<T>>) (Class<?>) List.class;
  }

  @Override
  public String toString() {
    return "List<" + elementTypeInfo + '>';
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    } else if (obj instanceof ListTypeInfo) {
      final ListTypeInfo<?> other = (ListTypeInfo<?>) obj;
      return elementTypeInfo.equals(other.elementTypeInfo);
    } else {
      return false;
    }
  }

  @Override
  public Object compatibleTo(TypeInfo<?> target, Object value) {
    if (TypeInfos.STRING_TYPE_INFO.getTypeClass() == target.getTypeClass()) {
      return JsonSerializer.serialize(value);
    }
    return super.compatibleTo(target, value);
  }

  public TypeInfo<T> getElementTypeInfo() {
    return this.elementTypeInfo;
  }

  @Override
  public int hashCode() {
    return 31 * elementTypeInfo.hashCode() + 1;
  }
}
