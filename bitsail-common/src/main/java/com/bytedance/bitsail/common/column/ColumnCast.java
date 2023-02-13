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

package com.bytedance.bitsail.common.column;

import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.exception.CommonErrorCode;
import com.bytedance.bitsail.common.option.CommonOptions;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.sql.Time;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.Date;
import java.util.List;
import java.util.Objects;

@Deprecated
public final class ColumnCast {

  private static final Logger LOG = LoggerFactory.getLogger(ColumnCast.class);

  private static String dateTimePattern;
  private static String datePattern;
  private static String timePattern;
  private static String zoneIdContent;
  private static String encoding;

  private static DateTimeFormatter dateTimeFormatter;
  private static DateTimeFormatter dateFormatter;
  private static DateTimeFormatter timeFormatter;
  private static List<DateTimeFormatter> formatters;
  private static ZoneId dateTimeZone;
  private static volatile boolean enabled = false;

  public static synchronized void initColumnCast(BitSailConfiguration commonConfiguration) {
    if (enabled) {
      return;
    }
    if (StringUtils.isEmpty(commonConfiguration.get(CommonOptions.DateFormatOptions.TIME_ZONE))) {
      zoneIdContent = ZoneOffset.systemDefault().getId();
    } else {
      zoneIdContent = commonConfiguration.get(CommonOptions.DateFormatOptions.TIME_ZONE);
    }
    dateTimeZone = ZoneId.of(zoneIdContent);
    dateTimePattern = commonConfiguration.get(CommonOptions.DateFormatOptions
        .DATE_TIME_PATTERN);
    datePattern = commonConfiguration.get(CommonOptions.DateFormatOptions
        .DATE_PATTERN);
    timePattern = commonConfiguration.get(CommonOptions.DateFormatOptions
        .TIME_PATTERN);
    encoding = commonConfiguration.get(CommonOptions.DateFormatOptions.COLUMN_ENCODING);

    formatters = Lists.newCopyOnWriteArrayList();
    dateTimeFormatter = DateTimeFormatter.ofPattern(dateTimePattern);
    dateFormatter = DateTimeFormatter.ofPattern(datePattern);
    timeFormatter = DateTimeFormatter.ofPattern(timePattern);
    formatters.add(dateTimeFormatter);
    formatters.add(dateFormatter);
    commonConfiguration.get(CommonOptions.DateFormatOptions.EXTRA_FORMATS)
        .forEach(pattern -> formatters.add(DateTimeFormatter.ofPattern(pattern)));
    enabled = true;
  }

  public static Date string2Date(StringColumn column) {
    checkState();
    if (null == column.asString()) {
      return null;
    }
    String dateStr = column.asString();

    for (DateTimeFormatter formatter : formatters) {
      try {
        TemporalAccessor parse = formatter.parse(dateStr);
        return fromTemporalAccessor(parse);
      } catch (Exception e) {
        LOG.debug("Formatter = {} parse string {} failed.", formatter, dateStr, e);
        //ignore
      }
    }
    throw new IllegalArgumentException(String.format("String [%s] can't be parse by all formatter.", dateStr));
  }

  private static Date fromTemporalAccessor(TemporalAccessor temporalAccessor) {
    LocalDate localDate = null;
    LocalTime localTime = null;

    if (temporalAccessor.isSupported(ChronoField.YEAR_OF_ERA)
        && temporalAccessor.isSupported(ChronoField.MONTH_OF_YEAR)
        && temporalAccessor.isSupported(ChronoField.DAY_OF_MONTH)) {
      localDate = LocalDate.of(
          temporalAccessor.get(ChronoField.YEAR_OF_ERA),
          temporalAccessor.get(ChronoField.MONTH_OF_YEAR),
          temporalAccessor.get(ChronoField.DAY_OF_MONTH));
    }
    if (temporalAccessor.isSupported(ChronoField.HOUR_OF_DAY)
        && temporalAccessor.isSupported(ChronoField.MINUTE_OF_HOUR)
        && temporalAccessor.isSupported(ChronoField.SECOND_OF_MINUTE)) {
      localTime = LocalTime.of(
          temporalAccessor.get(ChronoField.HOUR_OF_DAY),
          temporalAccessor.get(ChronoField.MINUTE_OF_HOUR),
          temporalAccessor.get(ChronoField.SECOND_OF_MINUTE),
          temporalAccessor.get(ChronoField.NANO_OF_SECOND)
      );
    }
    if (Objects.nonNull(localDate)) {
      LocalDateTime localDateTime;
      if (Objects.nonNull(localTime)) {
        localDateTime = LocalDateTime.of(localDate, localTime);
      } else {
        localDateTime = localDate.atStartOfDay();
      }
      return Date.from(localDateTime.atZone(dateTimeZone).toInstant());
    }
    if (Objects.nonNull(localTime)) {
      return new Time(
          localTime.getHour(),
          localTime.getMinute(),
          localTime.getSecond()
      );
    }
    throw BitSailException.asBitSailException(CommonErrorCode.CONVERT_NOT_SUPPORT,
        String.format("Temporal %s can't convert to date.", temporalAccessor));
  }

  public static String date2String(final DateColumn column) {
    checkState();
    if (null == column.asDate()) {
      return null;
    }
    DateColumn.DateType subType = column.getSubType();
    if (DateColumn.DateType.DATE.equals(subType)
        || DateColumn.DateType.TIME.equals(subType)
        || DateColumn.DateType.DATETIME.equals(subType)) {
      Date date = column.asDate();
      OffsetDateTime offsetDateTime = Instant.ofEpochMilli(date.toInstant().toEpochMilli())
          .atZone(dateTimeZone).toOffsetDateTime();
      switch (subType) {
        case DATE:
          return dateFormatter.format(offsetDateTime);
        case TIME:
          return timeFormatter.format(offsetDateTime);
        case DATETIME:
          return dateTimeFormatter.format(offsetDateTime);
        default:
          throw BitSailException
              .asBitSailException(CommonErrorCode.CONVERT_NOT_SUPPORT, "");
      }
    } else {
      Object rawData = column.getRawData();
      switch (subType) {
        case LOCAL_DATE:
          return dateFormatter.format(((LocalDate) rawData).atStartOfDay().atZone(dateTimeZone));
        case LOCAL_TIME:
          return timeFormatter.format((LocalTime) rawData);
        case LOCAL_DATE_TIME:
          return dateTimeFormatter.format(((LocalDateTime) rawData).atZone(dateTimeZone));
        default:
          throw BitSailException
              .asBitSailException(CommonErrorCode.CONVERT_NOT_SUPPORT, "");
      }
    }
  }

  public static String bytes2String(final BytesColumn column) throws UnsupportedEncodingException {
    checkState();
    if (null == column.asBytes()) {
      return null;
    }

    return new String(column.asBytes(), encoding);

  }

  public static void refresh() {
    enabled = false;
  }

  private static void checkState() {
    Preconditions.checkState(enabled, "Column cast in disabled from now.");
  }
}
