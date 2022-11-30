/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.bytedance.bitsail.connector.ftp.source.reader.deserializer;

import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.model.ColumnInfo;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.connector.ftp.error.FtpErrorCode;
import com.bytedance.bitsail.connector.ftp.option.FtpReaderOptions;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang.StringEscapeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class CsvRowDeserializer implements ITextRowDeserializer {
  private static final Logger LOG = LoggerFactory.getLogger(CsvRowDeserializer.class);

  private final List<Function<String, Object>> converters;
  private final int fieldSize;
  private final String csvDelimiter;
  private final Character csvMultiDelimiterReplaceChar;
  private boolean convertErrorColumnAsNull;

  private final CSVFormat csvFormat;

  private static final int LENGTH_SECOND = 10;
  private static final int LENGTH_MILLISECOND = 13;
  private static final int LENGTH_MICROSECOND = 16;
  private static final int LENGTH_NANOSECOND = 19;
  private static final int DAY_MILL =  24 * 3600 * 1000;
  private static final int ONE_THOUSAND = 1000;
  private static final int ONE_MILLION = 1000000;
  private static final String START_TIME = "1970-01-01";

  public CsvRowDeserializer(BitSailConfiguration jobConf) {
    List<ColumnInfo> columnInfos = jobConf.get(FtpReaderOptions.COLUMNS);
    this.converters = columnInfos.stream().map(this::initWrappedConverter).collect(Collectors.toList());
    this.fieldSize = converters.size();

    this.csvDelimiter = jobConf.get(FtpReaderOptions.CSV_DELIMITER);
    this.csvMultiDelimiterReplaceChar = jobConf.get(FtpReaderOptions.CSV_MULTI_DELIMITER_REPLACER);

    Character csvEscape = jobConf.getUnNecessaryOption(FtpReaderOptions.CSV_ESCAPE, null);
    Character csvQuote = jobConf.getUnNecessaryOption(FtpReaderOptions.CSV_QUOTE, null);
    String csvNullString = jobConf.getUnNecessaryOption(FtpReaderOptions.CSV_WITH_NULL_STRING, null);
    char csvFormatDelimiter = csvDelimiter.length() > 1 ? csvMultiDelimiterReplaceChar
            : csvDelimiter.charAt(0);
    this.csvFormat = CSVFormat.DEFAULT
            .withDelimiter(csvFormatDelimiter)
            .withEscape(csvEscape)
            .withQuote(csvQuote)
            .withNullString(csvNullString);
    this.convertErrorColumnAsNull = jobConf.get(FtpReaderOptions.CONVERT_ERROR_COLUMN_AS_NULL);
  }

  @Override
  public Row convert(String line) throws IOException {
    Row row = new Row(fieldSize);
    CSVParser parser = CSVParser.parse(line, csvFormat);
    CSVRecord csvRecord =  parser.getRecords().get(0);
    for (int i = 0; i < fieldSize; ++i) {
      String filedVal = StringEscapeUtils.unescapeCsv(csvRecord.get(i));
      row.setField(i, converters.get(i).apply(filedVal));
    }
    return row;
  }

  private Function<String, Object> initWrappedConverter(ColumnInfo columnInfo) {
    Function<String, Object> converter = initConverter(columnInfo);
    return filedVal -> {
      if (filedVal == null) {
        return  null;
      } else {
        try {
          return converter.apply(filedVal);
        } catch (BitSailException bitSailException) {
          throw (bitSailException);
        } catch (Exception e) {
          if (convertErrorColumnAsNull) {
            return null;
          } else {
            throw (e);
          }
        }
      }
    };
  }

  private Function<String, Object> initConverter(ColumnInfo columnInfo) {
    String typeName = columnInfo.getType().trim().toUpperCase();

    switch (typeName) {
      case "BOOLEAN":
        return filedVal -> Boolean.valueOf(filedVal);
      case "TINYINT":
        return filedVal -> Byte.parseByte(filedVal);
      case "SMALLINT":
        return filedVal -> Short.parseShort(filedVal);
      case "INT":
        return filedVal -> Integer.parseInt(filedVal);
      case "BIGINT":
      case "LONG":
        return filedVal -> Long.parseLong(filedVal);
      case "DATE":
        return filedVal -> stringToDate(filedVal);
      case "TIMESTAMP":
        return filedVal -> {
          try {
            Long.parseLong(filedVal);
            return new java.sql.Timestamp(getMillSecond(filedVal));
          } catch (NumberFormatException e) {
            // ignored
          }
          return new java.sql.Timestamp(stringToDate(filedVal).getTime());
        };
      case "FLOAT":
        return filedVal -> Float.parseFloat(filedVal);
      case "DOUBLE":
        return filedVal -> Double.parseDouble(filedVal);
      case "DECIMAL":
        return filedVal -> new BigDecimal(filedVal);
      case "CHAR":
      case "VARCHAR":
      case "STRING":
        return filedVal -> filedVal;
      case "STRING_UTF8":
      case "BINARY":
        return filedVal -> filedVal.getBytes();
      default:
        throw new BitSailException(FtpErrorCode.UNSUPPORTED_TYPE, "Type " + typeName + " is not supported");
    }
  }

  private  Date stringToDate(String strDate)  {
    if (strDate == null || strDate.trim().length() == 0) {
      return null;
    }

    try {
      LOG.debug("Trying parsing date str by standard_datetime_format: yyyy-MM-dd HH:mm:ss");
      return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(strDate);
    } catch (ParseException ignored) {
      // ignored
    }

    try {
      LOG.debug("Trying parsing date str by un_standard_datetime_format: yyyyMMddHHmmss");
      return new SimpleDateFormat("yyyyMMddHHmmss").parse(strDate);
    } catch (ParseException ignored) {
      // ignored
    }

    try {
      LOG.debug("Trying parsing date str by standard_datetime_format: yyyy-MM-dd");
      return new SimpleDateFormat("yyyy-MM-dd").parse(strDate);
    } catch (ParseException ignored) {
      // ignored
    }

    try {
      LOG.debug("Trying parsing date str by date_format: yyyyMMdd");
      return new SimpleDateFormat("yyyyMMdd").parse(strDate);
    } catch (ParseException ignored) {
      // ignored
    }

    try {
      LOG.debug("Trying parsing date str by time_format HH:mm:ss");
      return new SimpleDateFormat("HH:mm:ss").parse(strDate);
    } catch (ParseException ignored) {
      // ignored
    }

    try {
      LOG.debug("Trying parsing date str by year_format: yyyy");
      return new SimpleDateFormat("yyyy").parse(strDate);
    } catch (ParseException ignored) {
      // ignored
    }

    throw new RuntimeException("can't parse date");
  }

  private long getMillSecond(String data) {
    long time  = Long.parseLong(data);
    if (data.length() == LENGTH_SECOND) {
      time = Long.parseLong(data) * ONE_THOUSAND;
    } else if (data.length() == LENGTH_MILLISECOND) {
      time = Long.parseLong(data);
    } else if (data.length() == LENGTH_MICROSECOND) {
      time = Long.parseLong(data) / ONE_THOUSAND;
    } else if (data.length() == LENGTH_NANOSECOND) {
      time = Long.parseLong(data) / ONE_MILLION;
    } else if (data.length() < LENGTH_SECOND) {
      try {
        long day = Long.parseLong(data);
        Date date = new SimpleDateFormat("yyyy-MM-dd").parse(START_TIME);
        Calendar cal = Calendar.getInstance();
        long addMill = date.getTime() + day * DAY_MILL;
        cal.setTimeInMillis(addMill);
        time = cal.getTimeInMillis();
      } catch (Exception e) {
        throw new IllegalArgumentException("Can't convert " + data + " to MillSecond, Exception: " + e.toString());
      }
    }
    return time;
  }
}