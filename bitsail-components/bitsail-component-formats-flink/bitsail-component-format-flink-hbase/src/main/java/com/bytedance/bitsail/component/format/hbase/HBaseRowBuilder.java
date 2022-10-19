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

package com.bytedance.bitsail.component.format.hbase;

import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.column.BooleanColumn;
import com.bytedance.bitsail.common.column.BytesColumn;
import com.bytedance.bitsail.common.column.Column;
import com.bytedance.bitsail.common.column.DateColumn;
import com.bytedance.bitsail.common.column.DoubleColumn;
import com.bytedance.bitsail.common.column.LongColumn;
import com.bytedance.bitsail.common.column.StringColumn;
import com.bytedance.bitsail.common.exception.CommonErrorCode;
import com.bytedance.bitsail.component.format.api.RowBuilder;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.sql.Date;

public class HBaseRowBuilder implements RowBuilder {

  private static final Integer BIT_LENGTH = 1;

  @Override
  public void build(Object value, Row reuse, String mandatoryEncoding, RowTypeInfo rowTypeInfo) throws BitSailException {
    byte[][] rawData = (byte[][]) value;
    for (int i = 0; i < reuse.getArity(); i++) {
      TypeInformation<?> typeInfo = rowTypeInfo.getTypeAt(i);
      Column column = createColumn(typeInfo, mandatoryEncoding, rawData[i]);
      reuse.setField(i, column);
    }
  }

  public Column createColumn(TypeInformation<?> typeInformation, String encoding, byte[] byteArray) throws BitSailException {
    Class<?> columnTypeClass = typeInformation.getTypeClass();

    if (columnTypeClass == String.class) {
      try {
        return new StringColumn(byteArray == null ? null : new String(byteArray, encoding));
      } catch (UnsupportedEncodingException e) {
        throw BitSailException.asBitSailException(CommonErrorCode.UNSUPPORTED_ENCODING,
            "Unsupported encoding " + encoding);
      }
    } else if (columnTypeClass == Boolean.class) {
      // There is no boolean type in hbase.
      // So we use bytes.length>1 as boolean value.
      if (byteArray != null && byteArray.length > BIT_LENGTH) {
        return new BooleanColumn(Boolean.valueOf(Bytes.toString(byteArray)));
      }
      return new BooleanColumn(byteArray == null ? null : Bytes.toBoolean(byteArray));
    } else if (columnTypeClass == Integer.class || columnTypeClass == Short.class) {
      return new LongColumn(byteArray == null ? null : Integer.valueOf(Bytes.toString(byteArray)));
    } else if (columnTypeClass == Long.class || columnTypeClass == BigInteger.class) {
      return new LongColumn(byteArray == null ? null : Long.valueOf(Bytes.toString(byteArray)));
    } else if (columnTypeClass == Double.class || columnTypeClass == Float.class) {
      return new DoubleColumn(byteArray == null ? null : Double.valueOf(Bytes.toString(byteArray)));
    } else if (columnTypeClass == Date.class) {
      return new DateColumn(byteArray == null ? null : Long.valueOf(Bytes.toString(byteArray)));
    } else if (columnTypeClass == byte[].class) {
      return new BytesColumn(byteArray);
    } else {
      throw BitSailException.asBitSailException(CommonErrorCode.UNSUPPORTED_COLUMN_TYPE,
          columnTypeClass + " is not supported yet!");
    }
  }
}
