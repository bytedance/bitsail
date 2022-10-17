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

import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.MemorySegment;

import java.io.IOException;

/**
 * @desc:
 */
@SuppressWarnings("checkstyle:MagicNumber")
public class DateColumnComparator extends ColumnTypeComparator<DateColumn> {

  private static final long serialVersionUID = 1L;

  public DateColumnComparator(boolean ascending) {
    super(ascending);
  }

  public static int compareSerializedDate(DataInputView firstSource, DataInputView secondSource,
                                          boolean ascendingComparison) throws IOException {
    final long l1 = firstSource.readLong();
    final long l2 = secondSource.readLong();
    final int comp = (l1 < l2 ? -1 : (l1 == l2 ? 0 : 1));
    return ascendingComparison ? comp : -comp;
  }

  @Override
  public int compareSerialized(DataInputView firstSource, DataInputView secondSource)
      throws IOException {
    return compareSerializedDate(firstSource, secondSource, ascendingComparison);
  }

  @Override
  public boolean supportsNormalizedKey() {
    return true;
  }

  @Override
  public int getNormalizeKeyLen() {
    return 8;
  }

  @Override
  public boolean isNormalizedKeyPrefixOnly(int keyBytes) {
    return keyBytes < 8;
  }

  @Override
  public void putNormalizedKey(DateColumn record, MemorySegment target, int offset, int numBytes) {
    final long value = record.asDate().getTime() - Long.MIN_VALUE;

    // see IntValue for an explanation of the logic
    if (numBytes == 8) {
      // default case, full normalized key
      target.putLongBigEndian(offset, value);
    } else if (numBytes < 8) {
      for (int i = 0; numBytes > 0; numBytes--, i++) {
        target.put(offset + i, (byte) (value >>> ((7 - i) << 3)));
      }
    } else {
      target.putLongBigEndian(offset, value);
      for (int i = 8; i < numBytes; i++) {
        target.put(offset + i, (byte) 0);
      }
    }
  }

  @Override
  public TypeComparator<DateColumn> duplicate() {
    return new DateColumnComparator(ascendingComparison);
  }
}
