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
 *
 * Original Files: apache/flink(https://github.com/apache/flink)
 * Copyright: Copyright 2014-2022 The Apache Software Foundation
 * SPDX-License-Identifier: Apache License 2.0
 *
 * This file may have been modified by ByteDance Ltd. and/or its affiliates.
 */

package com.bytedance.bitsail.connector.legacy.jdbc.utils;

import java.time.temporal.ChronoUnit;

public class MicroIntervalUtil {
  public static final double DAYS_PER_MONTH_AVG = 365.25 / 12.0d;
  private static final int MONTHS_PER_YEAR = 12;
  private static final int HOURS_PER_DAY = 24;
  private static final int MINUTES_PER_HOUR = 60;
  private static final long MICROSECONDS_PER_SECOND = 1_000_000L;
  /**
   * Converts a number of time units to a duration in microseconds.
   *
   * @param years a number of years
   * @param months a number of months
   * @param days a number of days
   * @param hours a number of hours
   * @param minutes a number of minutes
   * @param seconds a number of seconds
   * @param micros a number of microseconds
   * @param daysPerMonthAvg an optional value representing a days per month average; if null, the default duration
   * from {@link ChronoUnit#MONTHS} is used.
   * @return @return Approximate representation of the given interval as a number of microseconds
   */
  public static long durationMicros(int years, int months, int days, int hours, int minutes, double seconds,
                                    int micros, Double daysPerMonthAvg) {
    if (daysPerMonthAvg == null) {
      daysPerMonthAvg = (double) ChronoUnit.MONTHS.getDuration().toDays();
    }
    double numberOfDays = ((years * MONTHS_PER_YEAR) + months) * daysPerMonthAvg + days;
    double numberOfSeconds =
        (((numberOfDays * HOURS_PER_DAY + hours) * MINUTES_PER_HOUR) + minutes) * MINUTES_PER_HOUR + seconds;
    return (long) (numberOfSeconds * MICROSECONDS_PER_SECOND + micros);
  }

  /**
   * Pad the string with the specific character to ensure the string is at least the specified length.
   *
   * @param original the string to be padded; may not be null
   * @param length the minimum desired length; must be positive
   * @param padChar the character to use for padding, if the supplied string is not long enough
   * @return the padded string of the desired length
   */
  public static String pad(String original,
                           int length,
                           char padChar) {
    if (original.length() >= length) {
      return original;
    }
    StringBuilder sb = new StringBuilder(original);
    while (sb.length() < length) {
      sb.append(padChar);
    }
    return sb.toString();
  }
}
