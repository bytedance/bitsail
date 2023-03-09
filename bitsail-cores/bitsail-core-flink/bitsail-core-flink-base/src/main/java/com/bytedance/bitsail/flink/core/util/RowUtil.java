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

package com.bytedance.bitsail.flink.core.util;

import com.bytedance.bitsail.common.column.Column;

import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class RowUtil {
  private static final Logger LOG = LoggerFactory.getLogger(RowUtil.class);

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
      return getBytesSize(field);
    }
  }

  private static long getBytesSize(Object field) {
    if (field == null) {
      return 0;
    }

    if (field instanceof List) {
      return getListBytesSize((List<?>) field);
    } else if (field instanceof Map) {
      return geMapBytesSize((Map<?, ?>) field);
    } else {
      return getNormalTypeByteSize(field);
    }
  }

  private static long getListBytesSize(List<?> fieldVal) {
    long listBytesSize = 0L;
    if (fieldVal != null) {
      for (Object o : fieldVal) {
        long elementBytesSize = getBytesSize(o);
        listBytesSize += elementBytesSize;
      }
    }
    return listBytesSize;
  }

  private static long geMapBytesSize(Map<?, ?> fieldVal) {
    long mapBytesSize = 0L;
    if (fieldVal != null) {
      for (Object key : fieldVal.keySet()) {
        mapBytesSize += getBytesSize(key);
        mapBytesSize += getBytesSize(fieldVal.get(key));
      }
    }

    return mapBytesSize;
  }

  @SuppressWarnings("checkstyle:MagicNumber")
  private static long getNormalTypeByteSize(Object field) {
    if (field == null) {
      return 0L;
    }

    Class<?> clazz = field.getClass();
    if (clazz == Boolean.class || clazz == Byte.class) {
      return 1L;
    } else if (clazz == Character.class || clazz == Short.class) {
      return 2L;
    } else if (clazz == Integer.class || clazz == Float.class) {
      return 4L;
    } else if (clazz == Long.class || clazz == Double.class) {
      return 8L;
    }

    if (clazz == String.class) {
      return ((String) field).length();
    }

    if (clazz == byte[].class) {
      return ((byte[]) field).length;
    }

    if (clazz == Byte[].class) {
      return ((Byte[]) field).length;
    }

    if (clazz == BigDecimal.class
        || clazz == BigInteger.class) {
      return 16L;
    }

    if (clazz.isAssignableFrom(Date.class)
        || clazz.isAssignableFrom(Timestamp.class)
        || clazz.isAssignableFrom(Time.class)) {
      return 12L;
    }

    if (clazz == LocalDate.class) {
      return 8L;
    }
    if (clazz == LocalTime.class) {
      return 7L;
    }
    if (clazz == LocalDateTime.class) {
      return 15L;
    }

    LOG.debug("Unsupported type [{}] for object value [{}]", clazz.getName(), field);
    return 0L;
  }
}
