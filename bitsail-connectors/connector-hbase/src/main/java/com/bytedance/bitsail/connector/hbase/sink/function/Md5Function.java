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

package com.bytedance.bitsail.connector.hbase.sink.function;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

public class Md5Function implements IFunction {

  private static final char[] DIGITS_LOWER = {'0', '1', '2', '3', '4', '5',
      '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

  @Override
  public String evaluate(Object str) {
    try {
      MessageDigest md5 = MessageDigest.getInstance("MD5");
      byte[] bytes = md5.digest(str.toString().getBytes(StandardCharsets.UTF_8));
      return bytes2Hex(bytes);
    } catch (Exception e) {
      throw new RuntimeException("Get md5 error:", e);
    }
  }

  @SuppressWarnings("checkstyle:MagicNumber")
  private static String bytes2Hex(final byte[] data) {
    final int l = data.length;
    final char[] out = new char[l << 1];
    // two characters form the hex value.
    for (int i = 0, j = 0; i < l; i++) {
      out[j++] = DIGITS_LOWER[(0xF0 & data[i]) >>> 4];
      out[j++] = DIGITS_LOWER[0x0F & data[i]];
    }
    return new String(out);
  }
}
