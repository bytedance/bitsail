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

package com.bytedance.bitsail.flink.core.typeinfo;

import com.bytedance.bitsail.common.column.Column;
import com.bytedance.bitsail.common.column.ListColumn;
import com.bytedance.bitsail.flink.core.typeutils.base.ListColumnSerializer;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link TypeInformation} for the list types of the Java API.
 *
 * @param <T> The type of the elements in the list.
 */
@PublicEvolving
@Deprecated
public final class ListColumnTypeInfo<T extends Column> extends TypeInformation<ListColumn<T>> {

  private static final long serialVersionUID = 1L;

  private final TypeInformation<T> elementTypeInfo;

  public ListColumnTypeInfo(Class<T> elementTypeClass) {
    this.elementTypeInfo = of(checkNotNull(elementTypeClass, "elementTypeClass"));
  }

  public ListColumnTypeInfo(TypeInformation<T> elementTypeInfo) {
    this.elementTypeInfo = checkNotNull(elementTypeInfo, "elementTypeInfo");
  }

  // ------------------------------------------------------------------------
  //  ListTypeInfo specific properties
  // ------------------------------------------------------------------------

  /**
   * Gets the type information for the elements contained in the list
   */
  public TypeInformation<T> getElementTypeInfo() {
    return elementTypeInfo;
  }

  // ------------------------------------------------------------------------
  //  TypeInformation implementation
  // ------------------------------------------------------------------------

  @Override
  public boolean isBasicType() {
    return false;
  }

  @Override
  public boolean isTupleType() {
    return false;
  }

  @Override
  public int getArity() {
    return 0;
  }

  @Override
  public int getTotalFields() {
    // similar as arrays, the lists are "opaque" to the direct field addressing logic
    // since the list's elements are not addressable, we do not expose them
    return 1;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Class<ListColumn<T>> getTypeClass() {
    return (Class<ListColumn<T>>) (Class<?>) List.class;
  }

  @Override
  public boolean isKeyType() {
    return false;
  }

  @Override
  public TypeSerializer<ListColumn<T>> createSerializer(ExecutionConfig config) {
    TypeSerializer<T> elementTypeSerializer = elementTypeInfo.createSerializer(config);
    return new ListColumnSerializer<T>(elementTypeSerializer, elementTypeInfo.getTypeClass());
  }

  // ------------------------------------------------------------------------

  @Override
  public String toString() {
    return "List<" + elementTypeInfo + '>';
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    } else if (obj instanceof ListColumnTypeInfo) {
      final ListColumnTypeInfo<?> other = (ListColumnTypeInfo<?>) obj;
      return other.canEqual(this) && elementTypeInfo.equals(other.elementTypeInfo);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return 31 * elementTypeInfo.hashCode() + 1;
  }

  @Override
  public boolean canEqual(Object obj) {
    return obj != null && obj.getClass() == getClass();
  }
}
