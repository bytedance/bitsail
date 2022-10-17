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
 *
 * Original Files: apache/flink(https://github.com/apache/flink)
 * Copyright: Copyright 2014-2022 The Apache Software Foundation
 * SPDX-License-Identifier: Apache License 2.0
 *
 * This file may have been modified by ByteDance Ltd. and/or its affiliates.
 */

package com.bytedance.bitsail.flink.core.typeutils.base;

import com.bytedance.bitsail.common.column.DateColumn;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/**
 * @desc:
 */
@Internal
public class DateColumnSerializer extends TypeSerializerSingleton<DateColumn> {

  public static final DateColumnSerializer INSTANCE = new DateColumnSerializer();
  private static final long serialVersionUID = 1L;

  @Override
  public boolean isImmutableType() {
    return false;
  }

  @Override
  public DateColumn createInstance() {
    return new DateColumn();
  }

  @Override
  public DateColumn copy(DateColumn from) {
    if (null == from) {
      return null;
    }
    DateColumn dc = new DateColumn(from.asLong());
    dc.setSubType(from.getSubType());
    return dc;
  }

  @Override
  public DateColumn copy(DateColumn from, DateColumn reuse) {
    return from;
  }

  @Override
  public void copy(DataInputView source, DataOutputView target) throws IOException {
    target.writeLong(source.readLong());
    target.writeLong(source.readInt());
  }

  @Override
  public int getLength() {
    return -1;
  }

  @Override
  public void serialize(DateColumn record, DataOutputView target) throws IOException {
    if (null == record || null == record.getRawData()) {
      target.writeLong(Long.MIN_VALUE);
    } else {
      target.writeLong(record.asLong());
    }
    target.writeInt(record.getSubType().ordinal());
  }

  @Override
  public DateColumn deserialize(DataInputView source) throws IOException {
    final long longValue = source.readLong();

    DateColumn dc;

    if (longValue == Long.MIN_VALUE) {
      dc = new DateColumn((Long) null);
    } else {
      dc = new DateColumn(longValue);
    }

    dc.setSubType(DateColumn.DateType.values()[source.readInt()]);
    return dc;
  }

  @Override
  public DateColumn deserialize(DateColumn reuse, DataInputView source) throws IOException {
    return deserialize(source);
  }

  @Override
  public TypeSerializerSnapshot<DateColumn> snapshotConfiguration() {
    return new DateColumnSerializerSnapshot();
  }

  // ------------------------------------------------------------------------

  /**
   * Serializer configuration snapshot for compatibility and format evolution.
   */
  @SuppressWarnings("WeakerAccess")
  public static final class DateColumnSerializerSnapshot extends SimpleTypeSerializerSnapshot<DateColumn> {

    public DateColumnSerializerSnapshot() {
      super(() -> INSTANCE);
    }
  }
}
