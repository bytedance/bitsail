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

package com.bytedance.bitsail.connector.hbase.format;

import com.bytedance.bitsail.base.format.DeserializationFormat;
import com.bytedance.bitsail.base.format.DeserializationSchema;
import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.exception.CommonErrorCode;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.common.typeinfo.BasicArrayTypeInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfos;

import org.apache.hadoop.hbase.util.Bytes;

import java.nio.charset.Charset;

public class HBaseDeserializationFormat implements DeserializationFormat<byte[][], Row> {

  private static final Integer BIT_LENGTH = 1;

  private BitSailConfiguration configuration;

  public HBaseDeserializationFormat(BitSailConfiguration configuration) {
    this.configuration = configuration;
  }

  @Override
  public DeserializationSchema<byte[][], Row> createRuntimeDeserializationSchema(TypeInfo<?>[] typeInfos) {
    return new DeserializationSchema<byte[][], Row>() {

      @Override
      public boolean isEndOfStream(Row nextElement) {
        return false;
      }

      @Override
      public Row deserialize(byte[][] rowCell) {
        Row row = new Row(typeInfos.length);
        for (int i = 0; i < row.getArity(); i++) {
          TypeInfo<?> typeInfo = typeInfos[i];
          row.setField(i, deserializeValue(typeInfo, rowCell[i]));
        }
        return row;
      }

      private Object deserializeValue(TypeInfo<?> typeInfo, byte[] cell) throws BitSailException {
        if (cell == null) {
          return null;
        }

        Class<?> columnTypeClass = typeInfo.getTypeClass();

        if (columnTypeClass == TypeInfos.STRING_TYPE_INFO.getTypeClass()) {
          return new String(cell, Charset.defaultCharset());

        } else if (columnTypeClass == TypeInfos.BOOLEAN_TYPE_INFO.getTypeClass()) {
          if (cell.length > BIT_LENGTH) {
            return Boolean.valueOf(Bytes.toString(cell));
          }
          return Bytes.toBoolean(cell);

        } else if (columnTypeClass == TypeInfos.INT_TYPE_INFO.getTypeClass() ||
                columnTypeClass == TypeInfos.SHORT_TYPE_INFO.getTypeClass()) {
          return Integer.valueOf(Bytes.toString(cell));

        } else if (columnTypeClass == TypeInfos.LONG_TYPE_INFO.getTypeClass() ||
                columnTypeClass == TypeInfos.BIG_INTEGER_TYPE_INFO.getTypeClass()) {
          return Long.valueOf(Bytes.toString(cell));

        } else if (columnTypeClass == TypeInfos.DOUBLE_TYPE_INFO.getTypeClass() ||
                columnTypeClass == TypeInfos.FLOAT_TYPE_INFO.getTypeClass()) {
          return Double.valueOf(Bytes.toString(cell));

        } else if (columnTypeClass == TypeInfos.SQL_DATE_TYPE_INFO.getTypeClass() ||
            columnTypeClass == TypeInfos.SQL_TIME_TYPE_INFO.getTypeClass() ||
            columnTypeClass == TypeInfos.SQL_TIMESTAMP_TYPE_INFO.getTypeClass()) {
          return Long.valueOf(Bytes.toString(cell));

        } else if (columnTypeClass == BasicArrayTypeInfo.BINARY_TYPE_INFO.getTypeClass()) {
          return cell;

        } else {
          throw BitSailException.asBitSailException(CommonErrorCode.UNSUPPORTED_COLUMN_TYPE,
            columnTypeClass + " is not supported yet!");
        }

      }
    };
  }
}
