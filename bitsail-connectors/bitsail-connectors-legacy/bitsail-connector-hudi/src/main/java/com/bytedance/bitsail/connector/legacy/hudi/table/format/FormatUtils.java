/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bytedance.bitsail.connector.legacy.hudi.table.format;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.generic.IndexedRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.apache.hudi.common.model.HoodieOperation;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;

/**
 * Utilities for format.
 */
public class FormatUtils {
  private FormatUtils() {
  }

  /**
   * Sets up the row kind to the row data {@code rowData} from the resolved operation.
   */
  public static void setRowKind(RowData rowData, IndexedRecord record, int index) {
    if (index == -1) {
      return;
    }
    rowData.setRowKind(getRowKind(record, index));
  }

  /**
   * Returns the RowKind of the given record, never null.
   * Returns RowKind.INSERT when the given field value not found.
   */
  private static RowKind getRowKind(IndexedRecord record, int index) {
    Object val = record.get(index);
    if (val == null) {
      return RowKind.INSERT;
    }
    final HoodieOperation operation = HoodieOperation.fromName(val.toString());
    if (HoodieOperation.isInsert(operation)) {
      return RowKind.INSERT;
    } else if (HoodieOperation.isUpdateBefore(operation)) {
      return RowKind.UPDATE_BEFORE;
    } else if (HoodieOperation.isUpdateAfter(operation)) {
      return RowKind.UPDATE_AFTER;
    } else if (HoodieOperation.isDelete(operation)) {
      return RowKind.DELETE;
    } else {
      throw new AssertionError();
    }
  }

  /**
   * Returns the RowKind of the given record, never null.
   * Returns RowKind.INSERT when the given field value not found.
   */
  public static RowKind getRowKindSafely(IndexedRecord record, int index) {
    if (index == -1) {
      return RowKind.INSERT;
    }
    return getRowKind(record, index);
  }

  public static GenericRecord buildAvroRecordBySchema(
      IndexedRecord record,
      Schema requiredSchema,
      int[] requiredPos,
      GenericRecordBuilder recordBuilder) {
    List<Schema.Field> requiredFields = requiredSchema.getFields();
    assert (requiredFields.size() == requiredPos.length);
    Iterator<Integer> positionIterator = Arrays.stream(requiredPos).iterator();
    requiredFields.forEach(f -> recordBuilder.set(f, getVal(record, positionIterator.next())));
    return recordBuilder.build();
  }

  private static Object getVal(IndexedRecord record, int pos) {
    return pos == -1 ? null : record.get(pos);
  }

  private static Boolean string2Boolean(String s) {
    return "true".equals(s.toLowerCase(Locale.ROOT));
  }
}
