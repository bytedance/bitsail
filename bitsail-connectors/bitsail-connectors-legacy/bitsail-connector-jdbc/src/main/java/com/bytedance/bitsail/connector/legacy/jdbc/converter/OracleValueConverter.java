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

package com.bytedance.bitsail.connector.legacy.jdbc.converter;

import com.bytedance.bitsail.connector.legacy.jdbc.utils.MicroIntervalUtil;

import oracle.jdbc.OracleResultSet;
import oracle.jdbc.OracleTypes;
import oracle.sql.INTERVALDS;
import oracle.sql.INTERVALYM;
import oracle.sql.TIMESTAMP;
import org.apache.commons.dbcp.DelegatingResultSet;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created 2022/10/18
 */
public class OracleValueConverter extends JdbcValueConverter {

  private static final Pattern INTERVAL_DAY_SECOND_PATTERN = Pattern.compile("([+\\-])?(\\d+) (\\d+):(\\d+):(\\d+).(\\d+)");
  private static final int PATTERN_GROUP_DAYS_IDX = 2;
  private static final int PATTERN_GROUP_HOURS_IDX = 3;
  private static final int PATTERN_GROUP_MINUTES_IDX = 4;
  private static final int PATTERN_GROUP_SECONDS_IDX = 5;
  private static final int PATTERN_GROUP_MICROSECONDS_IDX = 6;
  private static final int MICROS_LENGTH = 6;
  private IntervalHandlingMode intervalMode;

  public OracleValueConverter(IntervalHandlingMode mode) {
    this.intervalMode = mode;
  }

  private static OracleResultSet unwrap(ResultSet rs) {
    OracleResultSet oracleResultSet = null;
    if (rs instanceof OracleResultSet) {
      oracleResultSet = (OracleResultSet) rs;
    } else if (rs instanceof DelegatingResultSet) {
      oracleResultSet = unwrap(((DelegatingResultSet) rs).getInnermostDelegate());
    }

    return oracleResultSet;
  }

  @Override
  protected Object extract(ResultSet rs,
                           ResultSetMetaData metaData,
                           int columnIndex,
                           int columnType,
                           String columnTypeName,
                           String columnName,
                           String encoding) throws Exception {
    OracleResultSet oracleResultSet = unwrap(rs);
    if (Objects.isNull(oracleResultSet)) {
      return super.extract(rs, metaData, columnIndex, columnType, columnTypeName, columnName, encoding);
    }
    int oracleColumnType = metaData.getColumnType(columnIndex);
    switch (oracleColumnType) {
      case OracleTypes.TIMESTAMPTZ:
      case OracleTypes.TIMESTAMPLTZ:
        return getTimestampWithoutConnection(oracleResultSet, columnIndex);
      case OracleTypes.INTERVALDS:
        return getIntervalDSValue(oracleResultSet, columnIndex);
      case OracleTypes.INTERVALYM:
        return getIntervalYMValue(oracleResultSet, columnIndex);
      case OracleTypes.BINARY_FLOAT:
      case OracleTypes.BINARY_DOUBLE:
        return extractDoubleValue(oracleResultSet, columnIndex);
      default:
        return super.extract(rs, metaData, columnIndex, columnType, columnTypeName, columnName, encoding);
    }
  }

  @Override
  protected Object convert(Object value, int columnType, String columnName, String columnTypeName) throws Exception {
    switch (columnType) {
      case OracleTypes.TIMESTAMPTZ:
      case OracleTypes.TIMESTAMPLTZ:
        return convertTimeValue(value, columnName, columnTypeName);
      case OracleTypes.INTERVALDS:
        return convertIntervalDSValue((INTERVALDS) value, this.intervalMode);
      case OracleTypes.INTERVALYM:
        return convertIntervalYMValue((INTERVALYM) value, this.intervalMode);
      case OracleTypes.BINARY_FLOAT:
      case OracleTypes.BINARY_DOUBLE:
        return value;
      default:
        return super.convert(value, columnType, columnName, columnTypeName);
    }
  }

  private Timestamp getTimestampWithoutConnection(OracleResultSet rs,
                                                  int columnIndex) throws SQLException {
    TIMESTAMP timestamp = rs.getTIMESTAMP(columnIndex);
    return timestamp.timestampValue();
  }

  private Object getIntervalDSValue(OracleResultSet rs,
                                    int columnIndex) throws Exception {
    return rs.getINTERVALDS(columnIndex);
  }

  private Object getIntervalYMValue(OracleResultSet rs,
                                    int columnIndex) throws Exception {
    return rs.getINTERVALYM(columnIndex);
  }

  private Object convertIntervalDSValue(INTERVALDS interval, IntervalHandlingMode mode) throws Exception {
    final String intervalStr = interval.toString();
    if (mode.equals(IntervalHandlingMode.STRING)) {
      return intervalStr;
    } else if (mode.equals(IntervalHandlingMode.NUMERIC)) {
      final Matcher m = INTERVAL_DAY_SECOND_PATTERN.matcher(intervalStr);
      if (m.matches()) {
        final int sign = "-".equals(m.group(1)) ? -1 : 1;
        return MicroIntervalUtil.durationMicros(
                0,
                0,
                sign * Integer.parseInt(m.group(PATTERN_GROUP_DAYS_IDX)),
                sign * Integer.parseInt(m.group(PATTERN_GROUP_HOURS_IDX)),
                sign * Integer.parseInt(m.group(PATTERN_GROUP_MINUTES_IDX)),
                sign * Integer.parseInt(m.group(PATTERN_GROUP_SECONDS_IDX)),
                sign * Integer.parseInt(MicroIntervalUtil.pad(m.group(PATTERN_GROUP_MICROSECONDS_IDX), MICROS_LENGTH, '0')),
                MicroIntervalUtil.DAYS_PER_MONTH_AVG);
      }
    }
    throw new IllegalArgumentException("Fail to convert interval_day_to_seconds for oracle, mode: " + mode + " value: " + interval.toString());
  }

  private Object convertIntervalYMValue(INTERVALYM interval, IntervalHandlingMode mode) throws Exception {
    final String intervalStr = interval.toString();
    if (mode.equals(IntervalHandlingMode.STRING)) {
      return intervalStr;
    } else if (mode.equals(IntervalHandlingMode.NUMERIC)) {
      int sign = 1;
      int start = 0;
      if (intervalStr.charAt(0) == '-') {
        sign = -1;
        start = 1;
      }
      for (int i = 1; i < intervalStr.length(); i++) {
        if (intervalStr.charAt(i) == '-') {
          final int year = sign * Integer.parseInt(intervalStr.substring(start, i));
          final int month = sign * Integer.parseInt(intervalStr.substring(i + 1, intervalStr.length()));
          return MicroIntervalUtil.durationMicros(
                  year,
                  month,
                  0,
                  0,
                  0,
                  0,
                  0,
                  MicroIntervalUtil.DAYS_PER_MONTH_AVG);
        }
      }
    }

    throw new Exception("Fail to convert interval_year_to_month for oracle, mode: " + mode + " value: " + interval.toString());
  }

  /**
   * Oracle interval convert mode:
   *   NUMERIC: Convert to numeric. Unit: ms
   *   STRING: Convert to String
   */
  public enum IntervalHandlingMode {
    NUMERIC("numeric"),
    STRING("string");
    private final String value;

    IntervalHandlingMode(String value) {
      this.value = value;
    }

    /**
     * convert mode name into logical name
     * @param value mode value, may be null
     * @return the matchinig options
     */
    public static IntervalHandlingMode parse(String value) {
      if (value == null) {
        return null;
      }
      value = value.trim();
      for (IntervalHandlingMode option : IntervalHandlingMode.values()) {
        if (option.getValue().equalsIgnoreCase(value)) {
          return option;
        }
      }
      return null;
    }

    public String getValue() {
      return value;
    }
  }
}
