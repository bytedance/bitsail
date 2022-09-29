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

package com.bytedance.bitsail.common.column;

import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.exception.CommonErrorCode;
import com.bytedance.bitsail.common.option.CommonOptions;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.List;

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
  private static DateTimeZone dateTimeZone;
  private static volatile boolean enabled = false;

  public static void initColumnCast(BitSailConfiguration commonConfiguration) {
    if (enabled) {
      return;
    }
    if (StringUtils.isEmpty(commonConfiguration.get(CommonOptions.DateFormatOptions.TIME_ZONE))) {
      zoneIdContent = ZoneOffset.systemDefault().getId();
    } else {
      zoneIdContent = commonConfiguration.get(CommonOptions.DateFormatOptions.TIME_ZONE);
    }
    dateTimeZone = DateTimeZone.forID(zoneIdContent);
    dateTimePattern = commonConfiguration.get(CommonOptions.DateFormatOptions
        .DATE_TIME_PATTERN);
    datePattern = commonConfiguration.get(CommonOptions.DateFormatOptions
        .DATE_PATTERN);
    timePattern = commonConfiguration.get(CommonOptions.DateFormatOptions
        .TIME_PATTERN);
    encoding = commonConfiguration.get(CommonOptions.DateFormatOptions.COLUMN_ENCODING);

    formatters = Lists.newArrayList();
    dateTimeFormatter = DateTimeFormat.forPattern(dateTimePattern);
    dateFormatter = DateTimeFormat.forPattern(datePattern);
    timeFormatter = DateTimeFormat.forPattern(timePattern);
    commonConfiguration.get(CommonOptions.DateFormatOptions.EXTRA_FORMATS).forEach(pattern -> formatters.add(DateTimeFormat.forPattern(pattern)));
    formatters.add(dateTimeFormatter);
    formatters.add(dateFormatter);
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
        return formatter.parseDateTime(dateStr)
            .withZone(DateTimeZone.forID(zoneIdContent))
            .toDate();
      } catch (Exception e) {
        LOG.debug("Formatter = {} parse string {} failed.", formatter, dateStr, e);
        //ignore
      }
    }
    throw new IllegalArgumentException(String.format("String [%s] can't be parse by all formatter.", dateStr));
  }

  public static String date2String(final DateColumn column) {
    checkState();
    if (null == column.asDate()) {
      return null;
    }
    Date date = column.asDate();

    LocalDateTime localDateTime = LocalDateTime.fromDateFields(date);
    switch (column.getSubType()) {
      case DATE:
        return localDateTime.toString(dateFormatter);
      case TIME:
        return localDateTime.toString(timeFormatter);
      case DATETIME:
        return localDateTime.toString(dateTimeFormatter);
      default:
        throw BitSailException
            .asBitSailException(CommonErrorCode.CONVERT_NOT_SUPPORT, "");
    }
  }

  public static String bytes2String(final BytesColumn column) throws UnsupportedEncodingException {
    checkState();
    if (null == column.asBytes()) {
      return null;
    }

    return new String(column.asBytes(), encoding);

  }

  private static void checkState() {
    Preconditions.checkState(enabled, "Column cast in disabled from now.");
  }
}
