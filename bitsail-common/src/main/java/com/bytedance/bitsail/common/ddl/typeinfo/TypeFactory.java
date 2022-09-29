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

package com.bytedance.bitsail.common.ddl.typeinfo;

import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.exception.CommonErrorCode;

import org.apache.commons.lang3.reflect.FieldUtils;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public final class TypeFactory {

  /******************************** Basic ****************************************/
  public static final BasicTypeInfo<String> STRING_TYPE_INFO = new BasicTypeInfo<>(String.class);
  public static final BasicTypeInfo<Boolean> BOOLEAN_TYPE_INFO = new BasicTypeInfo<>(Boolean.class);
  public static final IntegerTypeInfo<Byte> BYTE_TYPE_INFO = new IntegerTypeInfo<>(Byte.class);
  public static final IntegerTypeInfo<Short> SHORT_TYPE_INFO = new IntegerTypeInfo<>(Short.class);
  public static final IntegerTypeInfo<Integer> INT_TYPE_INFO = new IntegerTypeInfo<>(Integer.class);
  public static final IntegerTypeInfo<Long> LONG_TYPE_INFO = new IntegerTypeInfo<>(Long.class);
  public static final FractionalTypeInfo<Float> FLOAT_TYPE_INFO = new FractionalTypeInfo<>(Float.class);
  public static final FractionalTypeInfo<Double> DOUBLE_TYPE_INFO = new FractionalTypeInfo<>(Double.class);
  public static final BasicTypeInfo<Character> CHAR_TYPE_INFO = new BasicTypeInfo<>(Character.class);
  public static final BasicTypeInfo<Date> DATE_TYPE_INFO = new BasicTypeInfo<>(Date.class);
  public static final BasicTypeInfo<Void> VOID_TYPE_INFO = new BasicTypeInfo<>(Void.class);
  public static final BasicTypeInfo<BigInteger> BIG_INT_TYPE_INFO = new BasicTypeInfo<>(BigInteger.class);
  public static final BasicTypeInfo<BigDecimal> BIG_DEC_TYPE_INFO = new BasicTypeInfo<>(BigDecimal.class);
  public static final BasicTypeInfo<Instant> INSTANT_TYPE_INFO = new BasicTypeInfo<>(Instant.class);

  /******************************** SQL TIME ****************************************/
  public static final SqlTimeTypeInfo<Date> DATE = new SqlTimeTypeInfo<>(Date.class);
  public static final SqlTimeTypeInfo<Time> TIME = new SqlTimeTypeInfo<>(Time.class);
  public static final SqlTimeTypeInfo<Timestamp> TIMESTAMP = new SqlTimeTypeInfo<>(Timestamp.class);

  /******************************** Array ****************************************/
  public static final PrimitiveArrayTypeInfo<boolean[]> BOOLEAN_PRIMITIVE_ARRAY_TYPE_INFO =
      new PrimitiveArrayTypeInfo<>(boolean[].class);
  public static final PrimitiveArrayTypeInfo<byte[]> BYTE_PRIMITIVE_ARRAY_TYPE_INFO =
      new PrimitiveArrayTypeInfo<>(byte[].class);
  public static final PrimitiveArrayTypeInfo<short[]> SHORT_PRIMITIVE_ARRAY_TYPE_INFO =
      new PrimitiveArrayTypeInfo<>(short[].class);
  public static final PrimitiveArrayTypeInfo<int[]> INT_PRIMITIVE_ARRAY_TYPE_INFO =
      new PrimitiveArrayTypeInfo<>(int[].class);
  public static final PrimitiveArrayTypeInfo<long[]> LONG_PRIMITIVE_ARRAY_TYPE_INFO =
      new PrimitiveArrayTypeInfo<>(long[].class);
  public static final PrimitiveArrayTypeInfo<float[]> FLOAT_PRIMITIVE_ARRAY_TYPE_INFO =
      new PrimitiveArrayTypeInfo<>(float[].class);
  public static final PrimitiveArrayTypeInfo<double[]> DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO =
      new PrimitiveArrayTypeInfo<>(double[].class);
  public static final PrimitiveArrayTypeInfo<char[]> CHAR_PRIMITIVE_ARRAY_TYPE_INFO =
      new PrimitiveArrayTypeInfo<>(char[].class);


  private static final Map<Class, TypeInfo> SUPPORTED_CLASS_MAP = new HashMap<>();

  static {
    try {
      Field[] fields = FieldUtils.getAllFields(TypeFactory.class);
      for (Field field : fields) {
        if (Modifier.isStatic(field.getModifiers())) {
          Object fieldValue = FieldUtils.readStaticField(TypeFactory.class, field.getName(), true);
          if (fieldValue instanceof TypeInfo) {
            TypeInfo supportedTypeInfo = (TypeInfo) fieldValue;
            SUPPORTED_CLASS_MAP.put(supportedTypeInfo.getTypeClass(), supportedTypeInfo);
          }
        }
      }
    } catch (Exception e) {
      throw BitSailException.asBitSailException(CommonErrorCode.INTERNAL_ERROR,
          String.format("initialize TypeFactory failed, maybe caused by reflect option. the reason is: %s",
              e.getMessage()));
    }
  }

  public static TypeInfo getTypeInformation(Class clazz) {
    if (!SUPPORTED_CLASS_MAP.containsKey(clazz)) {
      throw BitSailException.asBitSailException(CommonErrorCode.PLUGIN_ERROR,
          String.format("[%s] is not supported in custom type system.", clazz.getSimpleName()));
    }
    return SUPPORTED_CLASS_MAP.get(clazz);
  }
}
