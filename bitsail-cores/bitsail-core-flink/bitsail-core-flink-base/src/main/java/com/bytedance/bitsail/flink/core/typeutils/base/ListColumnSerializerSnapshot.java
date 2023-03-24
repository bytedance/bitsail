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

package com.bytedance.bitsail.flink.core.typeutils.base;

import com.bytedance.bitsail.common.column.Column;
import com.bytedance.bitsail.common.column.ListColumn;

import org.apache.flink.api.common.typeutils.CompositeTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;

/**
 * @class: ListColumnSerializerSnapshot
 * @desc: Snapshot class for the {@link ListColumnSerializer}.
 **/

@Deprecated
public class ListColumnSerializerSnapshot<T extends Column> extends CompositeTypeSerializerSnapshot<ListColumn<T>, ListColumnSerializer<T>> {
  private static final int CURRENT_VERSION = 1;

  // Type of list elements
  private final Class<T> valueClass;

  /**
   * Constructor for read instantiation.
   */
  public ListColumnSerializerSnapshot() {
    super(ListColumnSerializer.class);
    this.valueClass = null;
  }

  /**
   * Constructor to create the snapshot for writing.
   */
  public ListColumnSerializerSnapshot(ListColumnSerializer<T> listColumnSerializer, Class<T> elementTypeClass) {
    super(listColumnSerializer);
    this.valueClass = elementTypeClass;
  }

  @Override
  public int getCurrentOuterSnapshotVersion() {
    return CURRENT_VERSION;
  }

  @Override
  protected ListColumnSerializer<T> createOuterSerializerWithNestedSerializers(TypeSerializer<?>[] nestedSerializers) {
    @SuppressWarnings("unchecked")
    TypeSerializer<T> elementSerializer = (TypeSerializer<T>) nestedSerializers[0];
    return new ListColumnSerializer<T>(elementSerializer, this.valueClass);
  }

  @Override
  protected TypeSerializer<?>[] getNestedSerializers(ListColumnSerializer<T> outerSerializer) {
    return new TypeSerializer<?>[] {outerSerializer.getElementSerializer()};
  }
}
