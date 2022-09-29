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

package com.bytedance.bitsail.flink.core.util;

import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.column.Column;
import com.bytedance.bitsail.common.exception.CommonErrorCode;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.types.Row;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class RowUtil {

  public static long getRowBytesSize(Row row) {
    long totalBytes = 0L;
    for (int i = 0; i < row.getArity(); i++) {
      totalBytes += getFieldBytesSize(row.getField(i));
    }
    return totalBytes;
  }

  private static long getFieldBytesSize(Object field) {
    if (field instanceof Column) {
      return ((Column) field).getByteSize();
    } else {
      return getBytesSizeFromFlink(field);
    }
  }

  private static long getBytesSizeFromFlink(Object field) {
    if (field == null) {
      return 0;
    }

    if (field instanceof Tuple) {
      return getTupleBytesSizeFromFlink((Tuple) field);
    }

    if (field instanceof List) {
      return getListBytesSizeFromFlink((List) field);
    } else if (field instanceof Map) {
      return geMapBytesSizeFromFlink((Map) field);
    } else {
      return getBasicBytesSizeFromFlink(field);
    }
  }

  private static long getTupleBytesSizeFromFlink(Tuple fieldVal) {
    long tupleBytesSize = 0L;
    if (fieldVal != null) {
      for (int j = 0; j < fieldVal.getArity(); j++) {
        long elementBytesSize = getBytesSizeFromFlink(fieldVal.getField(j));
        tupleBytesSize += elementBytesSize;
      }
    }
    return tupleBytesSize;
  }

  private static long getListBytesSizeFromFlink(List<?> fieldVal) {
    long listBytesSize = 0L;
    if (fieldVal != null) {
      for (int j = 0; j < fieldVal.size(); j++) {
        long elementBytesSize = getBytesSizeFromFlink(fieldVal.get(j));
        listBytesSize += elementBytesSize;
      }
    }
    return listBytesSize;
  }

  private static long geMapBytesSizeFromFlink(Map<?, ?> fieldVal) {
    long mapBytesSize = 0L;
    if (fieldVal != null) {
      for (Object key : fieldVal.keySet()) {
        mapBytesSize += getBytesSizeFromFlink(key);
        mapBytesSize += getBytesSizeFromFlink(fieldVal.get(key));
      }
    }

    return mapBytesSize;
  }

  @SuppressWarnings("checkstyle:MagicNumber")
  private static long getBasicBytesSizeFromFlink(Object field) {
    if (field == null) {
      return 0;
    }

    if (field instanceof String) {
      return ((String) field).length();
    } else if (field instanceof Boolean) {
      return 1;
    } else if (field instanceof byte[]) {
      return ((byte[]) field).length;
    } else if (field instanceof Byte[]) {
      return ((Byte[]) field).length;
    } else if (field instanceof Byte) {
      return 1;
    } else if (field instanceof Date) {
      return 8;
    } else if (field instanceof BigInteger) {
      return 16;
    } else if (field instanceof Long) {
      return 8;
    } else if (field instanceof Integer) {
      return 4;
    } else if (field instanceof Short) {
      return 2;
    } else if (field instanceof Double) {
      return 8;
    } else if (field instanceof Float) {
      return 4;
    } else if (field instanceof BigDecimal) {
      return 16;
    } else {
      throw BitSailException.asBitSailException(CommonErrorCode.CONVERT_NOT_SUPPORT,
          "Flink data type " + field.getClass() + " is not supported!");
    }
  }
}
