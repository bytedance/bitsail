/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bytedance.bitsail.common.ddl.typeinfo;

import java.util.Objects;

public class BasicTypeInfo<T> extends TypeInfo<T> {

  private final Class<T> clazz;

  public BasicTypeInfo(Class<T> clazz) {
    this.clazz = clazz;
  }

  @Override
  public boolean isBasicType() {
    return true;
  }

  @Override
  public int getArity() {
    return 1;
  }

  @Override
  public Class<T> getTypeClass() {
    return this.clazz;
  }

  @Override
  public boolean canEqual(Object obj) {
    return obj instanceof BasicTypeInfo;
  }

  @Override
  public String toString() {
    return this.clazz.getSimpleName();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof BasicTypeInfo) {
      @SuppressWarnings("unchecked")
      BasicTypeInfo<T> other = (BasicTypeInfo<T>) obj;

      return other.canEqual(this) &&
          this.clazz == other.clazz;
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return 31 * Objects.hash(clazz);
  }

}
