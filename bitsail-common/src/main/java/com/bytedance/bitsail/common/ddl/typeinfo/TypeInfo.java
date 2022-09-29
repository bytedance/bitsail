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

import java.io.Serializable;

public abstract class TypeInfo<T> implements Serializable {

  /**
   * Checks if this type information represents a basic type.
   * Basic types are defined in {@link BasicTypeInfo} and are primitives, their boxing types,
   * Strings, Date, Void, ...
   *
   * @return True, if this type information describes a basic type, false otherwise.
   */
  public abstract boolean isBasicType();

  /**
   * Gets the arity of this type - the number of fields without nesting.
   *
   * @return Gets the number of fields in this type without nesting.
   */
  public abstract int getArity();

  /**
   * Gets the class of the type represented by this type information.
   *
   * @return The class of the type represented by this type information.
   */
  public abstract Class<T> getTypeClass();

  /**
   * Returns true if the given object can be equaled with this object. If not, it returns false.
   *
   * @param obj Object which wants to take part in the equality relation
   * @return true if obj can be equaled with this, otherwise false
   */
  public abstract boolean canEqual(Object obj);

  @Override
  public abstract String toString();

  @Override
  public abstract boolean equals(Object obj);

  @Override
  public abstract int hashCode();
}
