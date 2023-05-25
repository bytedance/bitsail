/*
 *       Copyright 2022-2023 Bytedance Ltd. and/or its affiliates.
 *
 *       Licensed under the Apache License, Version 2.0 (the "License");
 *       you may not use this file except in compliance with the License.
 *       You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *       Unless required by applicable law or agreed to in writing, software
 *       distributed under the License is distributed on an "AS IS" BASIS,
 *       WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *       See the License for the specific language governing permissions and
 *       limitations under the License.
 */

package com.bytedance.bitsail.common.typeinfo;

import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.exception.CommonErrorCode;
import com.bytedance.bitsail.common.option.CommonOptions;
import com.bytedance.bitsail.common.util.DateTimeFormatterUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashBasedTable;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Objects;
import java.util.function.Function;

public class TypeInfoCompatibles implements Serializable {

  private static final Long MAX_INTEGER_LONG_VALUE = (long) Integer.MAX_VALUE;
  private static final Long MIN_INTEGER_LONG_VALUE = (long) Integer.MIN_VALUE;

  private static final Long MAX_SHORT_LONG_VALUE = (long) Short.MAX_VALUE;
  private static final Long MIN_SHORT_LONG_VALUE = (long) Short.MIN_VALUE;

  private static final Integer MAX_SHORT_INTEGER_VALUE = (int) Short.MAX_VALUE;
  private static final Integer MIN_SHORT_INTEGER_VALUE = (int) Short.MIN_VALUE;

  private final BitSailConfiguration commonConfiguration;

  private final transient DateTimeFormatter dateFormatter;
  private final transient DateTimeFormatter timeFormatter;
  private final transient DateTimeFormatter dateTimeFormatter;
  private final transient ZoneId dateTimeZone;

  private final transient HashBasedTable<TypeInfo<?>, TypeInfo<?>, Function<Object, Object>> compatibles;

  public TypeInfoCompatibles(BitSailConfiguration commonConfiguration) {
    this.commonConfiguration = commonConfiguration;
    this.compatibles = HashBasedTable.create();

    this.dateFormatter = DateTimeFormatter.ofPattern(commonConfiguration.get(CommonOptions
        .DateFormatOptions.DATE_PATTERN));
    this.timeFormatter = DateTimeFormatter.ofPattern(commonConfiguration.get(CommonOptions
        .DateFormatOptions.TIME_PATTERN));
    this.dateTimeFormatter = DateTimeFormatter.ofPattern(commonConfiguration.get(CommonOptions
        .DateFormatOptions.DATE_TIME_PATTERN));

    this.dateTimeZone = commonConfiguration.fieldExists(CommonOptions.DateFormatOptions.TIME_ZONE) ?
        ZoneId.of(commonConfiguration.get(CommonOptions.DateFormatOptions.TIME_ZONE)) :
        ZoneId.systemDefault();

    addByteArrayTypeInfoCompatibles();
    addBooleanTypeInfoCompatibles();
    addStringTypeInfoCompatibles();
    addNumberTypeInfoCompatibles();
    addSqlDateTypeInfoCompatibles();
    addSqlTimeTypeInfoCompatibles();
    addSqlTimestampTypeInfoCompatibles();
    addLocalDateTypeInfoCompatibles();
    addLocalTimeTypeInfoCompatibles();
    addLocalDateTimeTypeInfoCompatibles();
  }

  private void addByteArrayTypeInfoCompatibles() {
    compatibles.put(BasicArrayTypeInfo.BINARY_TYPE_INFO,
        TypeInfos.STRING_TYPE_INFO,
        (value) -> new String((byte[]) value, Charset.defaultCharset())
    );
  }

  private void addLocalDateTimeTypeInfoCompatibles() {
    compatibles.put(TypeInfos.LOCAL_DATE_TIME_TYPE_INFO,
        TypeInfos.STRING_TYPE_INFO,
        (value) -> ((LocalDateTime) value).format(dateTimeFormatter)
    );

    compatibles.put(TypeInfos.LOCAL_DATE_TIME_TYPE_INFO,
        TypeInfos.LONG_TYPE_INFO,
        (value) -> ((LocalDateTime) value).atZone(dateTimeZone)
            .toInstant().toEpochMilli()
    );

    compatibles.put(TypeInfos.LOCAL_DATE_TIME_TYPE_INFO,
        TypeInfos.SQL_TIMESTAMP_TYPE_INFO,
        (value) -> Timestamp.valueOf((LocalDateTime) value)
    );

    compatibles.put(TypeInfos.LOCAL_DATE_TIME_TYPE_INFO,
        TypeInfos.LOCAL_DATE_TYPE_INFO,
        (value) -> ((LocalDateTime) value).toLocalDate()
    );

    compatibles.put(TypeInfos.LOCAL_DATE_TIME_TYPE_INFO,
        TypeInfos.LOCAL_TIME_TYPE_INFO,
        (value) -> ((LocalDateTime) value).toLocalTime()
    );

    compatibles.put(TypeInfos.LOCAL_DATE_TIME_TYPE_INFO,
        TypeInfos.SQL_DATE_TYPE_INFO,
        (value) -> java.sql.Date.valueOf(((LocalDateTime) value).toLocalDate())
    );

    compatibles.put(TypeInfos.LOCAL_DATE_TIME_TYPE_INFO,
        TypeInfos.SQL_TIME_TYPE_INFO,
        (value) -> java.sql.Time.valueOf(((LocalDateTime) value).toLocalTime())
    );
  }

  private void addLocalTimeTypeInfoCompatibles() {
    compatibles.put(TypeInfos.LOCAL_TIME_TYPE_INFO,
        TypeInfos.STRING_TYPE_INFO,
        (value) -> ((LocalTime) value).format(timeFormatter)
    );

    compatibles.put(TypeInfos.LOCAL_TIME_TYPE_INFO,
        TypeInfos.SQL_TIME_TYPE_INFO,
        (value) -> Time.valueOf((LocalTime) value)
    );
  }

  private void addLocalDateTypeInfoCompatibles() {
    compatibles.put(TypeInfos.LOCAL_DATE_TYPE_INFO,
        TypeInfos.STRING_TYPE_INFO,
        (value) -> ((LocalDate) value).format(dateFormatter)
    );

    compatibles.put(TypeInfos.LOCAL_DATE_TYPE_INFO,
        TypeInfos.SQL_DATE_TYPE_INFO,
        (value) -> Date.valueOf((LocalDate) value)
    );
  }

  private void addSqlTimestampTypeInfoCompatibles() {

    compatibles.put(TypeInfos.SQL_TIMESTAMP_TYPE_INFO,
        TypeInfos.LOCAL_DATE_TIME_TYPE_INFO,
        (value) -> ((Timestamp) value).toLocalDateTime()
    );

    compatibles.put(TypeInfos.SQL_TIMESTAMP_TYPE_INFO,
        TypeInfos.LOCAL_DATE_TYPE_INFO,
        new Function<Object, Object>() {
          @Override
          public Object apply(Object value) {
            return ((LocalDateTime) compatibles.get(TypeInfos.SQL_TIMESTAMP_TYPE_INFO, TypeInfos.LOCAL_DATE_TIME_TYPE_INFO)
                .apply(value)).toLocalDate();
          }
        }
    );

    compatibles.put(TypeInfos.SQL_TIMESTAMP_TYPE_INFO,
        TypeInfos.LOCAL_TIME_TYPE_INFO,
        new Function<Object, Object>() {
          @Override
          public Object apply(Object value) {
            return ((LocalDateTime) compatibles.get(TypeInfos.SQL_TIMESTAMP_TYPE_INFO, TypeInfos.LOCAL_DATE_TIME_TYPE_INFO)
                .apply(value)).toLocalTime();
          }
        }
    );

    compatibles.put(TypeInfos.SQL_TIMESTAMP_TYPE_INFO,
        TypeInfos.STRING_TYPE_INFO,
        new Function<Object, Object>() {
          @Override
          public Object apply(Object value) {
            return compatibles.get(TypeInfos.LOCAL_DATE_TIME_TYPE_INFO, TypeInfos.STRING_TYPE_INFO)
                .apply((compatibles.get(TypeInfos.SQL_TIMESTAMP_TYPE_INFO, TypeInfos.LOCAL_DATE_TIME_TYPE_INFO)
                    .apply(value)));
          }
        }
    );

    compatibles.put(TypeInfos.SQL_TIMESTAMP_TYPE_INFO,
        TypeInfos.LONG_TYPE_INFO,
        (value) -> ((Timestamp) value).getTime()
    );

    compatibles.put(TypeInfos.SQL_TIMESTAMP_TYPE_INFO,
        TypeInfos.SQL_DATE_TYPE_INFO,
        new Function<Object, Object>() {
          @Override
          public Object apply(Object value) {
            return new java.sql.Date(((Timestamp) value).getTime());
          }
        }
    );

    compatibles.put(TypeInfos.SQL_TIMESTAMP_TYPE_INFO,
        TypeInfos.SQL_TIME_TYPE_INFO,
        new Function<Object, Object>() {
          @Override
          public Object apply(Object value) {
            return new java.sql.Time(((Timestamp) value).getTime());
          }
        }
    );

  }

  private void addSqlTimeTypeInfoCompatibles() {
    compatibles.put(TypeInfos.SQL_TIME_TYPE_INFO,
        TypeInfos.LOCAL_TIME_TYPE_INFO,
        new Function<Object, Object>() {
          @Override
          public Object apply(Object value) {
            if (value instanceof java.sql.Time) {
              return ((java.sql.Time) value).toLocalTime();
            }
            //todo check
            java.util.Date date = (java.util.Date) value;
            return Instant.ofEpochMilli(date.getTime())
                .atZone(dateTimeZone)
                .toLocalTime();
          }
        }
    );

    compatibles.put(TypeInfos.SQL_TIME_TYPE_INFO,
        TypeInfos.STRING_TYPE_INFO,
        new Function<Object, Object>() {
          @Override
          public Object apply(Object value) {
            return compatibles.get(TypeInfos.LOCAL_TIME_TYPE_INFO, TypeInfos.STRING_TYPE_INFO)
                .apply((compatibles.get(TypeInfos.SQL_TIME_TYPE_INFO, TypeInfos.LOCAL_TIME_TYPE_INFO)
                    .apply(value)));
          }
        }
    );
    compatibles.put(TypeInfos.SQL_TIME_TYPE_INFO,
        TypeInfos.LONG_TYPE_INFO,
        (value) -> ((java.util.Date) value).getTime()
    );
  }

  private void addSqlDateTypeInfoCompatibles() {

    compatibles.put(TypeInfos.SQL_DATE_TYPE_INFO,
        TypeInfos.LOCAL_DATE_TYPE_INFO,
        new Function<Object, Object>() {
          @Override
          public Object apply(Object value) {
            if (value instanceof java.sql.Date) {
              return ((java.sql.Date) value).toLocalDate();
            }
            //todo check
            java.util.Date date = (java.util.Date) value;
            return Instant.ofEpochMilli(date.getTime())
                .atZone(dateTimeZone)
                .toLocalDate();
          }
        }
    );

    compatibles.put(TypeInfos.SQL_DATE_TYPE_INFO,
        TypeInfos.STRING_TYPE_INFO,
        new Function<Object, Object>() {
          @Override
          public Object apply(Object value) {
            return compatibles.get(TypeInfos.LOCAL_DATE_TYPE_INFO, TypeInfos.STRING_TYPE_INFO)
                .apply((compatibles.get(TypeInfos.SQL_DATE_TYPE_INFO, TypeInfos.LOCAL_DATE_TYPE_INFO)
                    .apply(value)));
          }
        }
    );

    compatibles.put(TypeInfos.SQL_DATE_TYPE_INFO,
        TypeInfos.LOCAL_DATE_TIME_TYPE_INFO,
        new Function<Object, Object>() {
          @Override
          public Object apply(Object value) {
            java.util.Date date = (java.util.Date) value;
            return Instant.ofEpochMilli(date.getTime())
                .atZone(dateTimeZone)
                .toLocalDateTime();
          }
        }
    );

    compatibles.put(TypeInfos.SQL_DATE_TYPE_INFO,
        TypeInfos.LONG_TYPE_INFO,
        new Function<Object, Object>() {
          @Override
          public Object apply(Object value) {
            java.util.Date date = (java.util.Date) value;
            return date.getTime();
          }
        }
    );
  }

  private void addNumberTypeInfoCompatibles() {
    compatibles.put(TypeInfos.SHORT_TYPE_INFO,
        TypeInfos.STRING_TYPE_INFO,
        (value) -> ((Short) value).toString()
    );

    compatibles.put(TypeInfos.SHORT_TYPE_INFO,
        TypeInfos.BYTE_TYPE_INFO,
        (value) -> ((Short) value).byteValue()
    );

    compatibles.put(TypeInfos.SHORT_TYPE_INFO,
        TypeInfos.BOOLEAN_TYPE_INFO,
        (value) -> ((Short) value) != 0
    );

    compatibles.put(TypeInfos.SHORT_TYPE_INFO,
        TypeInfos.INT_TYPE_INFO,
        (value) -> ((Short) value).intValue()
    );

    compatibles.put(TypeInfos.SHORT_TYPE_INFO,
        TypeInfos.LONG_TYPE_INFO,
        (value) -> ((Short) value).longValue()
    );

    compatibles.put(TypeInfos.SHORT_TYPE_INFO,
        TypeInfos.FLOAT_TYPE_INFO,
        (value) -> ((Short) value).floatValue()
    );

    compatibles.put(TypeInfos.SHORT_TYPE_INFO,
        TypeInfos.DOUBLE_TYPE_INFO,
        (value) -> ((Short) value).doubleValue()
    );

    //TODO in future, will be removed. short -> big integer
    compatibles.put(TypeInfos.SHORT_TYPE_INFO,
        TypeInfos.BIG_INTEGER_TYPE_INFO,
        new Function<Object, Object>() {
          @Override
          public Object apply(Object value) {

            try {
              return new BigInteger(((Short) value).toString());
            } catch (NumberFormatException e) {
              throw BitSailException.asBitSailException(
                  CommonErrorCode.CONVERT_NOT_SUPPORT,
                  String.format("short [%s] can't convert to big integer.", value));
            }
          }
        }
    );

    //TODO in future, will be removed. short -> big decimal
    compatibles.put(TypeInfos.SHORT_TYPE_INFO,
        TypeInfos.BIG_DECIMAL_TYPE_INFO,
        new Function<Object, Object>() {
          @Override
          public Object apply(Object value) {

            try {
              return new BigDecimal(((Short) value).toString());
            } catch (NumberFormatException e) {
              throw BitSailException.asBitSailException(
                  CommonErrorCode.CONVERT_NOT_SUPPORT,
                  String.format("short [%s] can't convert to big decimal.", value));
            }
          }
        }
    );

    compatibles.put(TypeInfos.INT_TYPE_INFO,
        TypeInfos.STRING_TYPE_INFO,
        (value) -> ((Integer) value).toString()
    );

    compatibles.put(TypeInfos.INT_TYPE_INFO,
        TypeInfos.SHORT_TYPE_INFO,
        new Function<Object, Object>() {
          @Override
          public Object apply(Object value) {
            Integer integer = (Integer) value;
            if (shortOverflow(integer)) {
              throw BitSailException.asBitSailException(
                  CommonErrorCode.CONVERT_OVER_FLOW,
                  String.format("int [%s] can't convert to short.", value));
            }
            return integer.shortValue();
          }
        }
    );

    compatibles.put(TypeInfos.INT_TYPE_INFO,
        TypeInfos.LONG_TYPE_INFO,
        (value) -> ((Integer) value).longValue()
    );

    compatibles.put(TypeInfos.INT_TYPE_INFO,
        TypeInfos.FLOAT_TYPE_INFO,
        (value) -> ((Integer) value).floatValue()
    );

    compatibles.put(TypeInfos.INT_TYPE_INFO,
        TypeInfos.DOUBLE_TYPE_INFO,
        (value) -> ((Integer) value).doubleValue()
    );

    compatibles.put(TypeInfos.INT_TYPE_INFO,
        TypeInfos.BOOLEAN_TYPE_INFO,
        (value) -> ((Integer) value) != 0
    );

    //TODO in future, will be removed. int -> big integer
    compatibles.put(TypeInfos.INT_TYPE_INFO,
        TypeInfos.BIG_INTEGER_TYPE_INFO,
        (value) -> BigInteger.valueOf(((Integer) value).longValue())
    );

    //TODO in future, will be removed. int -> big decimal
    compatibles.put(TypeInfos.INT_TYPE_INFO,
        TypeInfos.BIG_DECIMAL_TYPE_INFO,
        value -> BigDecimal.valueOf((Long) compatibles.get(TypeInfos.INT_TYPE_INFO, TypeInfos.LONG_TYPE_INFO)
            .apply(value))
    );

    compatibles.put(TypeInfos.LONG_TYPE_INFO,
        TypeInfos.STRING_TYPE_INFO,
        (value) -> ((Long) value).toString()
    );

    compatibles.put(TypeInfos.LONG_TYPE_INFO,
        TypeInfos.DOUBLE_TYPE_INFO,
        (value) -> ((Long) value).doubleValue()
    );

    compatibles.put(TypeInfos.LONG_TYPE_INFO,
        TypeInfos.BOOLEAN_TYPE_INFO,
        (value) -> ((Long) value) != 0L
    );

    compatibles.put(TypeInfos.LONG_TYPE_INFO,
        TypeInfos.INT_TYPE_INFO,
        new Function<Object, Object>() {
          @Override
          public Object apply(Object value) {
            Long longValue = (Long) value;
            if (integerOverflow(longValue)) {
              throw BitSailException.asBitSailException(
                  CommonErrorCode.CONVERT_OVER_FLOW,
                  String.format("long [%s] can't convert to int.", value));
            }
            return longValue.intValue();
          }
        }
    );

    compatibles.put(TypeInfos.LONG_TYPE_INFO,
        TypeInfos.SHORT_TYPE_INFO,
        //TODO overflow check
        new Function<Object, Object>() {
          @Override
          public Object apply(Object value) {
            Long longValue = (Long) value;
            if (shortOverflow(longValue)) {
              throw BitSailException.asBitSailException(
                  CommonErrorCode.CONVERT_OVER_FLOW,
                  String.format("long [%s] can't convert to short.", value));
            }
            return longValue.shortValue();
          }
        }
    );

    //only support milliseconds
    compatibles.put(TypeInfos.LONG_TYPE_INFO,
        TypeInfos.LOCAL_DATE_TIME_TYPE_INFO,
        new Function<Object, Object>() {
          @Override
          public Object apply(Object value) {

            try {
              return Instant.ofEpochMilli((Long) value).atZone(ZoneId.systemDefault())
                  .toLocalDateTime();
            } catch (Exception e) {
              throw BitSailException.asBitSailException(
                  CommonErrorCode.CONVERT_NOT_SUPPORT,
                  String.format("long [%s] can't convert to local date time.", value));
            }
          }
        }
    );

    compatibles.put(TypeInfos.LONG_TYPE_INFO,
        TypeInfos.SQL_TIMESTAMP_TYPE_INFO,
        new Function<Object, Object>() {
          @Override
          public Object apply(Object value) {
            try {
              return new Timestamp((Long) value);
            } catch (Exception e) {
              throw BitSailException.asBitSailException(
                  CommonErrorCode.CONVERT_NOT_SUPPORT,
                  String.format("long [%s] can't convert to timestamp", value));
            }
          }
        }
    );

    compatibles.put(TypeInfos.LONG_TYPE_INFO,
        TypeInfos.BIG_INTEGER_TYPE_INFO,
        value -> BigInteger.valueOf((Long) value)
    );

    compatibles.put(TypeInfos.LONG_TYPE_INFO,
        TypeInfos.BIG_DECIMAL_TYPE_INFO,
        value -> BigDecimal.valueOf((Long) value)
    );

    //use plain string will not parse big decimal as sci
    compatibles.put(TypeInfos.BIG_DECIMAL_TYPE_INFO,
        TypeInfos.STRING_TYPE_INFO,
        (value) -> ((BigDecimal) value).toPlainString()
    );

    compatibles.put(TypeInfos.BIG_DECIMAL_TYPE_INFO,
        TypeInfos.DOUBLE_TYPE_INFO,
        (value) -> ((BigDecimal) value).doubleValue()
    );

    //TODO in future we maybe discard this converted
    compatibles.put(TypeInfos.BIG_DECIMAL_TYPE_INFO,
        TypeInfos.FLOAT_TYPE_INFO,
        (value) -> ((BigDecimal) value).floatValue()
    );

    //TODO in future we maybe change to method `longValueExact` to avoid lose precisions
    compatibles.put(TypeInfos.BIG_DECIMAL_TYPE_INFO,
        TypeInfos.LONG_TYPE_INFO,
        (value) -> ((BigDecimal) value).longValue()
    );

    //TODO in future we maybe change to method `toBigIntegerExact` to avoid lose precisions
    compatibles.put(TypeInfos.BIG_DECIMAL_TYPE_INFO,
        TypeInfos.BIG_INTEGER_TYPE_INFO,
        (value) -> ((BigDecimal) value).toBigInteger()
    );

    compatibles.put(TypeInfos.BIG_INTEGER_TYPE_INFO,
        TypeInfos.STRING_TYPE_INFO,
        (value) -> ((BigInteger) value).toString()
    );

    compatibles.put(TypeInfos.BIG_INTEGER_TYPE_INFO,
        TypeInfos.LONG_TYPE_INFO,
        (value) -> ((BigInteger) value).longValueExact()
    );

    compatibles.put(TypeInfos.BIG_INTEGER_TYPE_INFO,
        TypeInfos.BIG_DECIMAL_TYPE_INFO,
        (value) -> new BigDecimal((BigInteger) value)
    );

    compatibles.put(TypeInfos.BIG_INTEGER_TYPE_INFO,
        TypeInfos.BOOLEAN_TYPE_INFO,
        new Function<Object, Object>() {
          @Override
          public Object apply(Object value) {
            try {
              return ((BigInteger) value).compareTo(BigInteger.ZERO) != 0;
            } catch (Exception e) {
              throw BitSailException.asBitSailException(
                  CommonErrorCode.CONVERT_NOT_SUPPORT,
                  String.format("biginteger [%s] can't convert to boolean", value));
            }
          }
        }
    );

    compatibles.put(TypeInfos.FLOAT_TYPE_INFO,
        TypeInfos.STRING_TYPE_INFO,
        (value) -> ((Float) value).toString()
    );

    compatibles.put(TypeInfos.FLOAT_TYPE_INFO,
        TypeInfos.BIG_DECIMAL_TYPE_INFO,
        value -> new BigDecimal((String) compatibles.get(TypeInfos.FLOAT_TYPE_INFO, TypeInfos.STRING_TYPE_INFO)
            .apply(value))
    );

    //TODO in future, will be removed. float -> biginteger
    compatibles.put(TypeInfos.FLOAT_TYPE_INFO,
        TypeInfos.BIG_INTEGER_TYPE_INFO,
        value -> ((BigDecimal) compatibles.get(TypeInfos.FLOAT_TYPE_INFO, TypeInfos.BIG_DECIMAL_TYPE_INFO)
            .apply(value)).toBigInteger()
    );

    compatibles.put(TypeInfos.FLOAT_TYPE_INFO,
        TypeInfos.DOUBLE_TYPE_INFO,
        value -> Double.valueOf((String) compatibles.get(TypeInfos.FLOAT_TYPE_INFO, TypeInfos.STRING_TYPE_INFO)
            .apply(value))
    );

    //TODO in future, will be removed. float -> long
    compatibles.put(TypeInfos.FLOAT_TYPE_INFO,
        TypeInfos.LONG_TYPE_INFO,
        (value) -> ((Float) value).longValue()
    );

    compatibles.put(TypeInfos.DOUBLE_TYPE_INFO,
        TypeInfos.STRING_TYPE_INFO,
        (value) -> ((Double) value).toString()
    );

    compatibles.put(TypeInfos.DOUBLE_TYPE_INFO,
        TypeInfos.BIG_DECIMAL_TYPE_INFO,
        value -> new BigDecimal((String) compatibles.get(TypeInfos.DOUBLE_TYPE_INFO, TypeInfos.STRING_TYPE_INFO)
            .apply(value))
    );

    //TODO in future, will be removed. double -> big integer now it will lose precisions
    compatibles.put(TypeInfos.DOUBLE_TYPE_INFO,
        TypeInfos.BIG_INTEGER_TYPE_INFO,
        value -> ((BigDecimal) compatibles.get(TypeInfos.DOUBLE_TYPE_INFO, TypeInfos.BIG_DECIMAL_TYPE_INFO)
            .apply(value)).toBigInteger()
    );

    //TODO in future, will be removed. double -> long
    compatibles.put(TypeInfos.DOUBLE_TYPE_INFO,
        TypeInfos.LONG_TYPE_INFO,
        (value) -> ((Double) value).longValue()
    );

    compatibles.put(TypeInfos.DOUBLE_TYPE_INFO,
        TypeInfos.FLOAT_TYPE_INFO,
        value -> ((Double) value).floatValue()
    );
  }

  private void addStringTypeInfoCompatibles() {
    compatibles.put(TypeInfos.STRING_TYPE_INFO,
        TypeInfos.BOOLEAN_TYPE_INFO,
        new Function<Object, Object>() {
          @Override
          public Object apply(Object value) {

            try {
              return Boolean.parseBoolean(value.toString());
            } catch (NumberFormatException e) {
              throw BitSailException.asBitSailException(
                  CommonErrorCode.CONVERT_NOT_SUPPORT,
                  String.format("string [%s] can't convert to boolean.", value));
            }
          }
        }
    );

    //TODO in future maybe add parameter to allow converter when we want to transform 1.2 to short
    compatibles.put(TypeInfos.STRING_TYPE_INFO,
        TypeInfos.SHORT_TYPE_INFO,
        new Function<Object, Object>() {
          @Override
          public Object apply(Object value) {

            try {
              return Short.parseShort(value.toString());
            } catch (NumberFormatException e) {
              throw BitSailException.asBitSailException(
                  CommonErrorCode.CONVERT_NOT_SUPPORT,
                  String.format("string [%s] can't convert to short.", value));
            }
          }
        }
    );

    //TODO in future maybe add parameter to allow converter when we want to transform 1.2 to int
    compatibles.put(TypeInfos.STRING_TYPE_INFO,
        TypeInfos.INT_TYPE_INFO,
        new Function<Object, Object>() {
          @Override
          public Object apply(Object value) {

            try {
              return Integer.parseInt(value.toString());
            } catch (NumberFormatException e) {
              throw BitSailException.asBitSailException(
                  CommonErrorCode.CONVERT_NOT_SUPPORT,
                  String.format("string [%s] can't convert to int.", value));
            }
          }
        }
    );

    //TODO in future maybe add parameter to allow converter when we want to transform 1.2 to long
    compatibles.put(TypeInfos.STRING_TYPE_INFO,
        TypeInfos.LONG_TYPE_INFO,
        new Function<Object, Object>() {
          @Override
          public Object apply(Object value) {

            try {
              return Long.parseLong(value.toString());
            } catch (NumberFormatException e) {
              throw BitSailException.asBitSailException(
                  CommonErrorCode.CONVERT_NOT_SUPPORT,
                  String.format("string [%s] can't convert to long.", value));
            }
          }
        }
    );

    compatibles.put(TypeInfos.STRING_TYPE_INFO,
        TypeInfos.FLOAT_TYPE_INFO,
        new Function<Object, Object>() {
          @Override
          public Object apply(Object value) {

            try {
              return Float.parseFloat(value.toString());
            } catch (NumberFormatException e) {
              throw BitSailException.asBitSailException(
                  CommonErrorCode.CONVERT_NOT_SUPPORT,
                  String.format("string [%s] can't convert to float.", value));
            }
          }
        }
    );
    compatibles.put(TypeInfos.STRING_TYPE_INFO,
        TypeInfos.DOUBLE_TYPE_INFO,
        new Function<Object, Object>() {
          @Override
          public Object apply(Object value) {

            try {
              return Double.parseDouble(value.toString());
            } catch (NumberFormatException e) {
              throw BitSailException.asBitSailException(
                  CommonErrorCode.CONVERT_NOT_SUPPORT,
                  String.format("string [%s] can't convert to double.", value));
            }
          }
        }
    );

    compatibles.put(TypeInfos.STRING_TYPE_INFO,
        TypeInfos.BIG_DECIMAL_TYPE_INFO,
        new Function<Object, Object>() {
          @Override
          public Object apply(Object value) {

            try {
              return new BigDecimal((String) value);
            } catch (NumberFormatException e) {
              throw BitSailException.asBitSailException(
                  CommonErrorCode.CONVERT_NOT_SUPPORT,
                  String.format("String[%s] can't convert to big decimal.", value));
            }
          }
        }
    );

    //TODO in future maybe add parameter to allow converter when we want to transform 1.2 to big integer
    compatibles.put(TypeInfos.STRING_TYPE_INFO,
        TypeInfos.BIG_INTEGER_TYPE_INFO,
        new Function<Object, Object>() {
          @Override
          public Object apply(Object value) {
            try {
              return new BigInteger((String) value);
            } catch (NumberFormatException e) {
              throw BitSailException.asBitSailException(
                  CommonErrorCode.CONVERT_NOT_SUPPORT,
                  String.format("String[%s] can't convert to big integer.", value));
            }
          }
        }
    );

    compatibles.put(TypeInfos.STRING_TYPE_INFO,
        TypeInfos.SQL_DATE_TYPE_INFO,
        new Function<Object, Object>() {
          @Override
          public Object apply(Object value) {
            try {
              LocalDate localDate = (LocalDate) (compatibles.get(TypeInfos.STRING_TYPE_INFO,
                  TypeInfos.LOCAL_DATE_TYPE_INFO).apply(value));
              return java.sql.Date.valueOf(localDate);
            } catch (Exception e) {
              try {
                LocalDateTime localDateTime = (LocalDateTime) (compatibles.get(TypeInfos.STRING_TYPE_INFO,
                    TypeInfos.LOCAL_DATE_TIME_TYPE_INFO).apply(value));
                return java.sql.Date.valueOf(localDateTime.toLocalDate());
              } catch (Exception e1) {
                throw BitSailException.asBitSailException(
                    CommonErrorCode.CONVERT_NOT_SUPPORT,
                    String.format("string [%s] can't convert to date.", value));
              }
            }
          }
        }
    );
    compatibles.put(TypeInfos.STRING_TYPE_INFO,
        TypeInfos.SQL_TIME_TYPE_INFO,
        new Function<Object, Object>() {
          @Override
          public Object apply(Object value) {
            try {
              LocalTime localTime = (LocalTime) (compatibles.get(TypeInfos.STRING_TYPE_INFO, TypeInfos.LOCAL_TIME_TYPE_INFO).apply(value));
              return Time.valueOf(localTime);
            } catch (Exception e) {
              try {
                LocalDateTime localDateTime = (LocalDateTime) (compatibles.get(TypeInfos.STRING_TYPE_INFO, TypeInfos.LOCAL_DATE_TIME_TYPE_INFO).apply(value));
                return Time.valueOf(localDateTime.toLocalTime());
              } catch (Exception e1) {
                throw BitSailException.asBitSailException(
                    CommonErrorCode.CONVERT_NOT_SUPPORT,
                    String.format("string [%s] can't convert to date.", value));
              }
            }
          }
        }
    );

    compatibles.put(TypeInfos.STRING_TYPE_INFO,
        TypeInfos.SQL_TIMESTAMP_TYPE_INFO,
        new Function<Object, Object>() {
          @Override
          public Object apply(Object value) {
            LocalDateTime localDateTime = (LocalDateTime) (compatibles.get(TypeInfos.STRING_TYPE_INFO, TypeInfos.LOCAL_DATE_TIME_TYPE_INFO).apply(value));
            return Timestamp.valueOf(localDateTime);
          }
        }
    );

    compatibles.put(TypeInfos.STRING_TYPE_INFO,
        TypeInfos.LOCAL_DATE_TIME_TYPE_INFO,
        new Function<Object, Object>() {
          @Override
          public Object apply(Object value) {
            String str = (String) value;
            try {
              return LocalDateTime.parse(str, dateTimeFormatter);
            } catch (Exception e) {
              str = DateTimeFormatterUtils.addSuffixNecessary(str);
              DateTimeFormatter formatter = DateTimeFormatterUtils.getFormatter(str);
              if (Objects.isNull(formatter)) {
                throw e;
              }
              return LocalDateTime.parse(str, formatter);
            }
          }
        }
    );

    compatibles.put(TypeInfos.STRING_TYPE_INFO,
        TypeInfos.LOCAL_DATE_TYPE_INFO,
        new Function<Object, Object>() {
          @Override
          public Object apply(Object value) {
            try {
              return LocalDate.parse((String) value, dateFormatter);
            } catch (Exception e) {
              return ((LocalDateTime) compatibles.get(TypeInfos.STRING_TYPE_INFO, TypeInfos.LOCAL_DATE_TIME_TYPE_INFO)
                  .apply(value)).toLocalDate();
            }
          }
        }
    );

    compatibles.put(TypeInfos.STRING_TYPE_INFO,
        TypeInfos.LOCAL_TIME_TYPE_INFO,
        new Function<Object, Object>() {
          @Override
          public Object apply(Object value) {
            try {
              return LocalTime.parse((String) value, timeFormatter);
            } catch (Exception e) {
              return ((LocalDateTime) compatibles.get(TypeInfos.STRING_TYPE_INFO, TypeInfos.LOCAL_DATE_TIME_TYPE_INFO)
                  .apply(value)).toLocalTime();
            }
          }
        }
    );

    compatibles.put(TypeInfos.STRING_TYPE_INFO,
        BasicArrayTypeInfo.BINARY_TYPE_INFO,
        (value) -> ((String) value).getBytes(StandardCharsets.UTF_8)
    );
  }

  private void addBooleanTypeInfoCompatibles() {
    compatibles.put(TypeInfos.BOOLEAN_TYPE_INFO,
        TypeInfos.STRING_TYPE_INFO,
        (value) -> ((Boolean) value).toString()
    );
    compatibles.put(TypeInfos.BOOLEAN_TYPE_INFO,
        TypeInfos.INT_TYPE_INFO,
        (value) -> ((Boolean) value) ? 1 : 0
    );
    compatibles.put(TypeInfos.BOOLEAN_TYPE_INFO,
        TypeInfos.SHORT_TYPE_INFO,
        (value) -> ((Boolean) value) ? 1 : 0
    );
    compatibles.put(TypeInfos.BOOLEAN_TYPE_INFO,
        TypeInfos.LONG_TYPE_INFO,
        (value) -> ((Boolean) value) ? 1L : 0L
    );
    compatibles.put(TypeInfos.BOOLEAN_TYPE_INFO,
        TypeInfos.FLOAT_TYPE_INFO,
        (value) -> ((Boolean) value) ? 1f : 0f
    );
    compatibles.put(TypeInfos.BOOLEAN_TYPE_INFO,
        TypeInfos.DOUBLE_TYPE_INFO,
        (value) -> ((Boolean) value) ? 1d : 0d
    );

    //TODO in future, will be removed. boolean -> big integer
    compatibles.put(TypeInfos.BOOLEAN_TYPE_INFO,
        TypeInfos.BIG_INTEGER_TYPE_INFO,
        new Function<Object, Object>() {
          @Override
          public Object apply(Object o) {
            return BigInteger.valueOf((long) compatibles.get(TypeInfos.BOOLEAN_TYPE_INFO, TypeInfos.LONG_TYPE_INFO)
                .apply(o));
          }
        }
    );

    //TODO in future, will be removed. boolean -> big decimal
    compatibles.put(TypeInfos.BOOLEAN_TYPE_INFO,
        TypeInfos.BIG_DECIMAL_TYPE_INFO,
        new Function<Object, Object>() {
          @Override
          public Object apply(Object o) {
            return BigDecimal.valueOf((long) compatibles.get(TypeInfos.BOOLEAN_TYPE_INFO, TypeInfos.LONG_TYPE_INFO)
                .apply(o));
          }
        }
    );
  }

  public Object compatibleTo(TypeInfo<?> from,
                             TypeInfo<?> to,
                             Object value) {
    Class<?> fromTypeClass = from.getTypeClass();
    Class<?> toTypeClass = to.getTypeClass();
    if (fromTypeClass == toTypeClass) {
      return value;
    }
    Function<Object, Object> function = compatibles.get(from, to);
    if (Objects.isNull(function)) {
      throw BitSailException.asBitSailException(CommonErrorCode.CONVERT_NOT_SUPPORT,
          String.format("Type %s not compatible with another type %s.", from, to));
    }
    try {
      return function.apply(value);
    } catch (BitSailException e) {
      throw e;
    } catch (Exception e) {
      throw BitSailException.asBitSailException(
          CommonErrorCode.CONVERT_NOT_SUPPORT,
          String.format("Type %s's value [%s] can't convert to another type %s.", from, value, to), e);
    }
  }

  private static boolean integerOverflow(long longValue) {
    return longValue > MAX_INTEGER_LONG_VALUE || longValue < MIN_INTEGER_LONG_VALUE;
  }

  private static boolean shortOverflow(long longValue) {
    return longValue > MAX_SHORT_LONG_VALUE || longValue < MIN_SHORT_LONG_VALUE;
  }

  private static boolean shortOverflow(int integerValue) {
    return integerValue > MAX_SHORT_INTEGER_VALUE || integerValue < MIN_SHORT_INTEGER_VALUE;
  }

  @VisibleForTesting
  public HashBasedTable<TypeInfo<?>, TypeInfo<?>, Function<Object, Object>> getCompatibles() {
    return compatibles;
  }

}
