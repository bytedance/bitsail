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
import com.bytedance.bitsail.common.util.DateUtil;
import com.bytedance.bitsail.connector.ftp.error.FtpErrorCode;
import com.bytedance.bitsail.connector.ftp.option.FtpReaderOptions;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang.StringEscapeUtils;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class CsvRowDeserializer implements ITextRowDeserializer {

  private final List<Function<String, Object>> converters;
  private final int fieldSize;
  private final String csvDelimiter;
  private final Character csvMultiDelimiterReplaceChar;
  private boolean convertErrorColumnAsNull;

  private final CSVFormat csvFormat;
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
        return filedVal -> DateUtil.stringToDate(filedVal, null, null);
      case "TIMESTAMP":
        return filedVal -> {
          try {
            Integer.parseInt(filedVal);
            return new java.sql.Timestamp(DateUtil.getMillSecond(filedVal));
          } catch (NumberFormatException e) {
            // ignored
          }
          try {
            Long.parseLong(filedVal);
            return new java.sql.Timestamp(DateUtil.getMillSecond(filedVal));
          } catch (NumberFormatException e) {
            // ignored
          }
          return DateUtil.columnToTimestamp(filedVal, null, null);
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
}