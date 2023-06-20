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

package com.bytedance.bitsail.connector.kudu.util;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.PartitionSchema;
import org.apache.kudu.util.ByteVec;
import org.apache.kudu.util.DateUtil;
import org.apache.kudu.util.DecimalUtil;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;

public class KuduKeyEncoderUtils {
  @SuppressWarnings("checkstyle:MagicNumber")
  private static final BigInteger MIN_VALUE_128 = BigInteger.valueOf(-2).pow(127);

  /**
   * Decodes a range partition key into a partial row and other columns set null.
   *
   * @param schema the schema of the table
   * @param partitionSchema the partition schema of the table
   * @param buf the encoded range partition key
   * @return the decoded range key
   */
  public static PartialRow decodeRangePartitionKey(Schema schema,
                                                    PartitionSchema partitionSchema,
                                                    ByteBuffer buf) {
    PartialRow row = schema.newPartialRow();
    Iterator <Integer> rangeIds = partitionSchema.getRangeSchema().getColumnIds().iterator();
    while (rangeIds.hasNext()) {
      int idx = schema.getColumnIndex(rangeIds.next());
      if (buf.hasRemaining()) {
        decodeColumn(buf, row, idx, !rangeIds.hasNext());
      } else {
        row.setNull(idx);
      }
    }

    if (buf.hasRemaining()) {
      throw new IllegalArgumentException("Unable to decode all partition key bytes");
    }
    return row;
  }

  /**
   * Decoded a key-encoded column into a row.
   *
   * @param buf the buffer containing the column
   * @param row the row to set the column value in
   * @param idx the index of the column to decode
   * @param isLast whether the column is the last column in the key
   */
  public static void decodeColumn(ByteBuffer buf, PartialRow row, int idx, boolean isLast) {
    Schema schema = row.getSchema();
    ColumnSchema column = schema.getColumnByIndex(idx);
    switch (column.getType()) {
      case INT8:
        row.addByte(idx, (byte) (buf.get() ^ Byte.MIN_VALUE));
        break;
      case INT16:
        row.addShort(idx, (short) (buf.getShort() ^ Short.MIN_VALUE));
        break;
      case DATE: {
        int days = buf.getInt() ^ Integer.MIN_VALUE;
        row.addDate(idx, DateUtil.epochDaysToSqlDate(days));
        break;
      }
      case INT32:
        row.addInt(idx, buf.getInt() ^ Integer.MIN_VALUE);
        break;
      case INT64:
      case UNIXTIME_MICROS:
        row.addLong(idx, buf.getLong() ^ Long.MIN_VALUE);
        break;
      case BINARY: {
        byte[] binary = decodeBinaryColumn(buf, isLast);
        row.addBinary(idx, binary);
        break;
      }
      case VARCHAR: {
        byte[] binary = decodeBinaryColumn(buf, isLast);
        row.addVarchar(idx, new String(binary, StandardCharsets.UTF_8));
        break;
      }
      case STRING: {
        byte[] binary = decodeBinaryColumn(buf, isLast);
        row.addStringUtf8(idx, binary);
        break;
      }
      case DECIMAL: {
        int scale = column.getTypeAttributes().getScale();
        int size = column.getTypeSize();
        switch (size) {
          case  DecimalUtil.DECIMAL32_SIZE:
            int intVal = buf.getInt() ^ Integer.MIN_VALUE;
            row.addDecimal(idx, BigDecimal.valueOf(intVal, scale));
            break;
          case DecimalUtil.DECIMAL64_SIZE:
            long longVal = buf.getLong() ^ Long.MIN_VALUE;
            row.addDecimal(idx, BigDecimal.valueOf(longVal, scale));
            break;
          case DecimalUtil.DECIMAL128_SIZE:
            byte[] bytes = new byte[size];
            buf.get(bytes);
            BigInteger bigIntVal = new BigInteger(bytes).xor(MIN_VALUE_128);
            row.addDecimal(idx, new BigDecimal(bigIntVal, scale));
            break;
          default:
            throw new IllegalArgumentException("Unsupported decimal type size: " + size);
        }
        break;
      }
      default:
        throw new IllegalArgumentException(String.format(
            "The column type %s is not a valid key component type",
            schema.getColumnByIndex(idx).getType()));
    }
  }

  /**
   * Decode a binary key column.
   *
   * @param key the key bytes
   * @param isLast whether the column is the final column in the key.
   * @return the binary value.
   */
  public static byte[] decodeBinaryColumn(ByteBuffer key, boolean isLast) {
    if (isLast) {
      byte[] bytes = Arrays.copyOfRange(key.array(),
          key.arrayOffset() + key.position(),
          key.arrayOffset() + key.limit());
      key.position(key.limit());
      return bytes;
    }

    // When encoding a binary column that is not the final column in the key, a
    // 0x0000 separator is used to retain lexicographic comparability. Null
    // bytes in the input are escaped as 0x0001.
    ByteVec buf = ByteVec.withCapacity(key.remaining());
    for (int i = key.position(); i < key.limit(); i++) {
      if (key.get(i) == 0) {
        switch (key.get(i + 1)) {
          case 0: {
            buf.append(key.array(),
                key.arrayOffset() + key.position(),
                i - key.position());
            key.position(i + 2);
            return buf.toArray();
          }
          case 1: {
            buf.append(key.array(),
                key.arrayOffset() + key.position(),
                i + 1 - key.position());
            i++;
            key.position(i + 1);
            break;
          }
          default: throw new IllegalArgumentException("Unexpected binary sequence");
        }
      }
    }

    buf.append(key.array(),
        key.arrayOffset() + key.position(),
        key.remaining());
    key.position(key.limit());
    return buf.toArray();
  }

}
