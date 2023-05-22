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

package com.bytedance.bitsail.common.typeinfo;

import com.google.common.base.Preconditions;

public class GenericTypeInfo<T> extends TypeInfo<T> {

  private final Class<T> typeClass;

  public GenericTypeInfo(Class<T> typeClass) {
    this.typeClass = Preconditions.checkNotNull(typeClass);
  }

  @Override
  public Class<T> getTypeClass() {
    return typeClass;
  }

  @Override
  public int hashCode() {
    return typeClass.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof GenericTypeInfo) {
      @SuppressWarnings("unchecked")
      GenericTypeInfo<T> genericTypeInfo = (GenericTypeInfo<T>) obj;

      return typeClass == genericTypeInfo.typeClass;
    } else {
      return false;
    }
  }

  @Override
  public String toString() {
    return "GenericType<" + typeClass.getCanonicalName() + ">";
  }
}
