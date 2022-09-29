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

import com.bytedance.bitsail.common.util.Preconditions;

import java.util.Objects;

public class PrimitiveArrayTypeInfo<T> extends TypeInfo<T> {

  private final Class<T> arrayClass;

  protected PrimitiveArrayTypeInfo(Class<T> arrayClass) {
    Preconditions.checkNotNull(arrayClass);
    this.arrayClass = arrayClass;

  }

  @Override
  public boolean isBasicType() {
    return false;
  }

  @Override
  public int getArity() {
    return 1;
  }

  @Override
  public Class<T> getTypeClass() {
    return this.arrayClass;
  }

  @Override
  public boolean canEqual(Object obj) {
    return obj instanceof PrimitiveArrayTypeInfo;
  }

  @Override
  public String toString() {
    return arrayClass.getComponentType().getName() + "[]";
  }

  @Override
  public boolean equals(Object obj) {
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(arrayClass);
  }
}
