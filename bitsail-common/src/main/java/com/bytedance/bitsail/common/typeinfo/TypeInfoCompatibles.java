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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashBasedTable;
import org.apache.commons.lang3.math.NumberUtils;

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
        //TODO check time zone.
        (value) -> ((LocalDateTime) value).atZone(dateTimeZone)
            .toInstant().toEpochMilli()
    );

    compatibles.put(TypeInfos.LOCAL_DATE_TIME_TYPE_INFO,
        TypeInfos.SQL_TIMESTAMP_TYPE_INFO,
        //TODO check time zone.
        (value) -> Timestamp.valueOf((LocalDateTime) value)
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
            return dateTimeFormatter.format(((LocalDateTime) compatibles.get(TypeInfos.SQL_TIMESTAMP_TYPE_INFO, TypeInfos.LOCAL_DATE_TIME_TYPE_INFO)
                .apply(value)));
          }
        }
    );
    compatibles.put(TypeInfos.SQL_TIMESTAMP_TYPE_INFO,
        TypeInfos.LONG_TYPE_INFO,
        (value) -> ((Timestamp) value).getTime()
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
            return timeFormatter.format(((LocalTime) compatibles.get(TypeInfos.SQL_TIME_TYPE_INFO, TypeInfos.LOCAL_TIME_TYPE_INFO)
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
            return dateFormatter.format(((LocalDate) compatibles.get(TypeInfos.SQL_DATE_TYPE_INFO, TypeInfos.LOCAL_DATE_TYPE_INFO)
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
        (value) -> ((Short) value) == 1
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
        (value) -> ((Integer) value) == 1
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
        (value) -> ((Long) value) == 1L
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

    //compatibles.put(TypeInfos.LONG_TYPE_INFO,
    //    TypeInfos.BIG_INTEGER_TYPE_INFO,
    //    null
    //);

    //compatibles.put(TypeInfos.LONG_TYPE_INFO,
    //    TypeInfos.BIG_DECIMAL_TYPE_INFO,
    //    null
    //);
    //

    compatibles.put(TypeInfos.BIG_DECIMAL_TYPE_INFO,
        TypeInfos.STRING_TYPE_INFO,
        (value) -> ((BigDecimal) value).toString()
    );

    compatibles.put(TypeInfos.BIG_DECIMAL_TYPE_INFO,
        TypeInfos.DOUBLE_TYPE_INFO,
        (value) -> ((BigDecimal) value).doubleValue()
    );

    compatibles.put(TypeInfos.BIG_DECIMAL_TYPE_INFO,
        TypeInfos.FLOAT_TYPE_INFO,
        (value) -> ((BigDecimal) value).floatValue()
    );

    compatibles.put(TypeInfos.BIG_INTEGER_TYPE_INFO,
        TypeInfos.STRING_TYPE_INFO,
        (value) -> ((BigInteger) value).toString()
    );

    compatibles.put(TypeInfos.FLOAT_TYPE_INFO,
        TypeInfos.STRING_TYPE_INFO,
        (value) -> ((Float) value).toString()
    );

    compatibles.put(TypeInfos.FLOAT_TYPE_INFO,
        TypeInfos.BIG_DECIMAL_TYPE_INFO,
        new Function<Object, Object>() {
          @Override
          public Object apply(Object value) {
            return new BigDecimal((String) compatibles.get(TypeInfos.FLOAT_TYPE_INFO, TypeInfos.STRING_TYPE_INFO)
                .apply(value));
          }
        }
    );

    compatibles.put(TypeInfos.FLOAT_TYPE_INFO,
        TypeInfos.DOUBLE_TYPE_INFO,
        new Function<Object, Object>() {
          @Override
          public Object apply(Object value) {
            return Double.valueOf((String) compatibles.get(TypeInfos.FLOAT_TYPE_INFO, TypeInfos.STRING_TYPE_INFO)
                .apply(value));
          }
        }
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
        new Function<Object, Object>() {
          @Override
          public Object apply(Object value) {

            try {
              return BigDecimal.valueOf(((Double) value));
            } catch (NumberFormatException e) {
              throw BitSailException.asBitSailException(
                  CommonErrorCode.CONVERT_NOT_SUPPORT,
                  String.format("double [%s] can't convert to big decimal.", value));
            }
          }
        }
    );

    //TODO in future, will be removed. double -> big integer
    compatibles.put(TypeInfos.DOUBLE_TYPE_INFO,
        TypeInfos.BIG_INTEGER_TYPE_INFO,
        new Function<Object, Object>() {
          @Override
          public Object apply(Object value) {

            try {
              return BigDecimal.valueOf(((Double) value)).toBigInteger();
            } catch (NumberFormatException e) {
              throw BitSailException.asBitSailException(
                  CommonErrorCode.CONVERT_NOT_SUPPORT,
                  String.format("double [%s] can't convert to big integer.", value));
            }
          }
        }
    );

    //TODO in future, will be removed. double -> long
    compatibles.put(TypeInfos.DOUBLE_TYPE_INFO,
        TypeInfos.LONG_TYPE_INFO,
        (value) -> ((Double) value).longValue()
    );
  }

  private void addStringTypeInfoCompatibles() {
    compatibles.put(TypeInfos.STRING_TYPE_INFO,
        TypeInfos.SHORT_TYPE_INFO,
        new Function<Object, Object>() {
          @Override
          public Object apply(Object value) {

            try {
              return NumberUtils.toShort(value.toString());
            } catch (NumberFormatException e) {
              throw BitSailException.asBitSailException(
                  CommonErrorCode.CONVERT_NOT_SUPPORT,
                  String.format("string [%s] can't convert to short.", value));
            }
          }
        }
    );
    compatibles.put(TypeInfos.STRING_TYPE_INFO,
        TypeInfos.INT_TYPE_INFO,
        new Function<Object, Object>() {
          @Override
          public Object apply(Object value) {

            try {
              return NumberUtils.toInt(value.toString());
            } catch (NumberFormatException e) {
              throw BitSailException.asBitSailException(
                  CommonErrorCode.CONVERT_NOT_SUPPORT,
                  String.format("string [%s] can't convert to int.", value));
            }
          }
        }
    );
    compatibles.put(TypeInfos.STRING_TYPE_INFO,
        TypeInfos.LONG_TYPE_INFO,
        new Function<Object, Object>() {
          @Override
          public Object apply(Object value) {

            try {
              return NumberUtils.toLong(value.toString());
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
              return NumberUtils.toFloat(value.toString());
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
              return NumberUtils.toDouble(value.toString());
            } catch (NumberFormatException e) {
              throw BitSailException.asBitSailException(
                  CommonErrorCode.CONVERT_NOT_SUPPORT,
                  String.format("string [%s] can't convert to double.", value));
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
              LocalDate localDate = (LocalDate) (compatibles.get(TypeInfos.STRING_TYPE_INFO, TypeInfos.LOCAL_TIME_TYPE_INFO).apply(value));
              long timestamp = localDate.atStartOfDay()
                  .atZone(dateTimeZone)
                  .toInstant()
                  .toEpochMilli();
              return new Date(timestamp);
            } catch (Exception e) {
              try {
                LocalDateTime localDateTime = (LocalDateTime) (compatibles.get(TypeInfos.STRING_TYPE_INFO, TypeInfos.LOCAL_DATE_TIME_TYPE_INFO).apply(value));
                long timestamp = localDateTime
                    .atZone(dateTimeZone)
                    .toInstant()
                    .toEpochMilli();
                return new Date(timestamp);
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
              return new Time(localTime.getHour(), localTime.getMinute(), localTime.getSecond());
            } catch (Exception e) {
              try {
                LocalDateTime localDateTime = (LocalDateTime) (compatibles.get(TypeInfos.STRING_TYPE_INFO, TypeInfos.LOCAL_DATE_TIME_TYPE_INFO).apply(value));
                return new Time(localDateTime.getHour(), localDateTime.getMinute(), localDateTime.getSecond());
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
            return new Timestamp(localDateTime.atZone(dateTimeZone)
                .toInstant()
                .toEpochMilli());
          }
        }
    );

    compatibles.put(TypeInfos.STRING_TYPE_INFO,
        TypeInfos.LOCAL_DATE_TIME_TYPE_INFO,
        new Function<Object, Object>() {
          @Override
          public Object apply(Object value) {
            return LocalDateTime.parse((String) value, dateTimeFormatter);
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

    compatibles.put(TypeInfos.STRING_TYPE_INFO,
        TypeInfos.BIG_INTEGER_TYPE_INFO,
        new Function<Object, Object>() {
          @Override
          public Object apply(Object value) {
            try {
              return ((BigDecimal) compatibles.get(TypeInfos.STRING_TYPE_INFO, TypeInfos.BIG_DECIMAL_TYPE_INFO)
                  .apply(value)).toBigInteger();
            } catch (NumberFormatException e) {
              throw BitSailException.asBitSailException(
                  CommonErrorCode.CONVERT_NOT_SUPPORT,
                  String.format("String[%s] can't convert to big integer.", value));
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
