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

package com.bytedance.bitsail.component.format.csv;

import com.bytedance.bitsail.base.format.DeserializationSchema;
import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.option.CommonOptions;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.common.typeinfo.BasicArrayTypeInfo;
import com.bytedance.bitsail.common.typeinfo.RowTypeInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfos;
import com.bytedance.bitsail.component.format.csv.error.CsvFormatErrorCode;
import com.bytedance.bitsail.component.format.csv.option.CsvReaderOptions;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang.StringEscapeUtils;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.util.Arrays;

public class CsvDeserializationSchema implements DeserializationSchema<byte[], Row> {

  private final BitSailConfiguration deserializationConfiguration;
  private final String csvDelimiter;
  private final Character csvMultiDelimiterReplaceChar;
  private boolean convertErrorColumnAsNull;


  private final transient CSVFormat csvFormat;

  private final transient DateTimeFormatter localDateTimeFormatter;
  private final transient DateTimeFormatter localDateFormatter;
  private final transient DateTimeFormatter localTimeFormatter;
  private final transient DeserializationConverter<CSVRecord, Row> typeInfoConverter;

  public CsvDeserializationSchema(BitSailConfiguration deserializationConfiguration,
                                  RowTypeInfo rowTypeInfo) {
    this.deserializationConfiguration = deserializationConfiguration;
    this.csvDelimiter = deserializationConfiguration.get(CsvReaderOptions.CSV_DELIMITER);
    this.csvMultiDelimiterReplaceChar = deserializationConfiguration.get(CsvReaderOptions.CSV_MULTI_DELIMITER_REPLACER);

    Character csvEscape = deserializationConfiguration.getUnNecessaryOption(CsvReaderOptions.CSV_ESCAPE, null);
    Character csvQuote = deserializationConfiguration.getUnNecessaryOption(CsvReaderOptions.CSV_QUOTE, null);
    String csvNullString = deserializationConfiguration.getUnNecessaryOption(CsvReaderOptions.CSV_WITH_NULL_STRING, null);
    char csvFormatDelimiter = csvDelimiter.length() > 1 ? csvMultiDelimiterReplaceChar
        : csvDelimiter.charAt(0);
    this.csvFormat = CSVFormat.DEFAULT
        .withDelimiter(csvFormatDelimiter)
        .withEscape(csvEscape)
        .withQuote(csvQuote)
        .withNullString(csvNullString);
    this.localDateTimeFormatter = DateTimeFormatter.ofPattern(
        deserializationConfiguration.get(CommonOptions.DateFormatOptions.DATE_TIME_PATTERN));
    this.localDateFormatter = DateTimeFormatter
        .ofPattern(deserializationConfiguration.get(CommonOptions.DateFormatOptions.DATE_PATTERN));
    this.localTimeFormatter = DateTimeFormatter
        .ofPattern(deserializationConfiguration.get(CommonOptions.DateFormatOptions.TIME_PATTERN));

    this.typeInfoConverter = createConverters(rowTypeInfo.getTypeInfos(), rowTypeInfo.getFieldNames());
    this.convertErrorColumnAsNull = deserializationConfiguration.get(CsvReaderOptions.CONVERT_ERROR_COLUMN_AS_NULL);

  }

  @Override
  public Row deserialize(byte[] message) {
    CSVRecord csvRecord;
    try {
      String inputStr = new String(message);
      String csvMultiDelimiterReplaceString = csvMultiDelimiterReplaceChar.toString();
      if ((csvDelimiter.length() > 1) && inputStr.contains(csvMultiDelimiterReplaceString)) {
        throw new BitSailException(CsvFormatErrorCode.CSV_FORMAT_SCHEMA_PARSE_FAILED,
            String.format("Input row contains '%c', the csv_multi_delimiter_replace_char option should be set to other character e.g. '⊙'.",
                csvMultiDelimiterReplaceChar));
      } else if (csvDelimiter.length() > 1) {
        inputStr = inputStr.replace(csvDelimiter, csvMultiDelimiterReplaceString);
      }
      CSVParser parser = CSVParser.parse(inputStr, csvFormat);
      csvRecord = parser.getRecords().get(0);
    } catch (Exception e) {
      throw BitSailException.asBitSailException(CsvFormatErrorCode.CSV_FORMAT_SCHEMA_PARSE_FAILED, e);
    }

    return (Row) typeInfoConverter.convert(csvRecord);
  }

  @Override
  public boolean isEndOfStream(Row nextElement) {
    return false;
  }

  private interface DeserializationConverter<I, O> extends Serializable {
    O convert(I input);
  }

  private DeserializationConverter createConverters(TypeInfo<?>[] typeInfos,
                                                    String[] fieldNames) {
    final DeserializationConverter<String, Object>[] fieldConverters = Arrays.stream(typeInfos)
        .map(this::createConverter)
        .toArray(DeserializationConverter[]::new);

    return csvRecord -> {
      CSVRecord record = (CSVRecord) csvRecord;
      int arity = fieldNames.length;
      Row row = new Row(arity);
      for (int i = 0; i < arity; i++) {
        String fieldName = fieldNames[i];
        String filed = StringEscapeUtils.unescapeCsv(record.get(i));
        Object converted;
        try {
          converted = fieldConverters[i].convert(filed);
        } catch (Exception e) {
          throw BitSailException.asBitSailException(CsvFormatErrorCode.CSV_FORMAT_COVERT_FAILED,
              String.format("Field %s can't convert into type %s, value = %s.",
                  fieldName,
                  typeInfos[i],
                  filed));
        }
        row.setField(i, converted);
      }
      return row;
    };
  }

  private DeserializationConverter<String, Object> createConverter(TypeInfo<?> typeInfo) {
    return wrapperCreateConverter(typeInfo);
  }

  private DeserializationConverter<String, Object> wrapperCreateConverter(TypeInfo<?> typeInfo) {
    DeserializationConverter<String, Object> typeInfoConverter =
        createTypeInfoConverter(typeInfo);
    return (field) -> {
      if (field == null) {
        return null;
      }

      try {
        return typeInfoConverter.convert(field);
      } catch (Throwable t) {
        if (convertErrorColumnAsNull) {
          return null;
        } else {
          throw t;
        }
      }
    };
  }

  private DeserializationConverter<String, Object> createTypeInfoConverter(TypeInfo<?> typeInfo) {
    Class<?> typeClass = typeInfo.getTypeClass();

    if (typeClass == TypeInfos.VOID_TYPE_INFO.getTypeClass()) {
      return field -> null;
    }
    if (typeClass == TypeInfos.BOOLEAN_TYPE_INFO.getTypeClass()) {
      return this::convertToBoolean;
    }
    if (typeClass == TypeInfos.SHORT_TYPE_INFO.getTypeClass()) {
      return this::convertToShort;
    }
    if (typeClass == TypeInfos.INT_TYPE_INFO.getTypeClass()) {
      return this::convertToInt;
    }
    if (typeClass == TypeInfos.LONG_TYPE_INFO.getTypeClass()) {
      return this::convertToLong;
    }
    if (typeClass == TypeInfos.BIG_INTEGER_TYPE_INFO.getTypeClass()) {
      return this::convertToBigInteger;
    }
    if (typeClass == TypeInfos.FLOAT_TYPE_INFO.getTypeClass()) {
      return this::convertToFloat;
    }
    if (typeClass == TypeInfos.DOUBLE_TYPE_INFO.getTypeClass()) {
      return this::convertToDouble;
    }
    if (typeClass == TypeInfos.BIG_DECIMAL_TYPE_INFO.getTypeClass()) {
      return this::createDecimalConverter;
    }
    if (typeClass == TypeInfos.STRING_TYPE_INFO.getTypeClass()) {
      return this::convertToString;
    }
    if (typeClass == TypeInfos.SQL_TIMESTAMP_TYPE_INFO.getTypeClass()) {
      return this::convertToSqlTimestamp;
    }
    if (typeClass == TypeInfos.SQL_DATE_TYPE_INFO.getTypeClass()) {
      return this::convertToSqlTime;
    }
    if (typeClass == TypeInfos.SQL_TIME_TYPE_INFO.getTypeClass()) {
      return this::convertToSqlDate;
    }
    if (typeClass == TypeInfos.LOCAL_DATE_TIME_TYPE_INFO.getTypeClass()) {
      return this::convertToTimestamp;
    }
    if (typeClass == TypeInfos.LOCAL_DATE_TYPE_INFO.getTypeClass()) {
      return this::convertToDate;
    }
    if (typeClass == TypeInfos.LOCAL_TIME_TYPE_INFO.getTypeClass()) {
      return this::convertToTime;
    }
    if (typeClass == BasicArrayTypeInfo.BINARY_TYPE_INFO.getTypeClass()) {
      return this::convertToBytes;
    }
    throw BitSailException.asBitSailException(CsvFormatErrorCode.CSV_FORMAT_COVERT_FAILED,
        String.format("Csv format converter not support type info: %s.", typeInfo));
  }

  private Short convertToShort(String field) {
    return Short.parseShort(field.trim());
  }

  private Date convertToSqlDate(String field) {
    LocalDate localDate = convertToDate(field);
    return new Date(localDate.getYear(), localDate.getMonthValue(), localDate.getDayOfMonth());
  }

  private Time convertToSqlTime(String field) {
    throw new UnsupportedOperationException();
  }

  private Timestamp convertToSqlTimestamp(String field) {
    LocalDateTime localDateTime = convertToTimestamp(field);
    return new Timestamp(localDateTime
        .atZone(ZoneOffset.systemDefault())
        .toInstant().toEpochMilli());
  }

  private boolean convertToBoolean(String field) {
    return Boolean.parseBoolean(field.trim());
  }

  private int convertToInt(String field) {
    return Integer.parseInt(field.trim());
  }

  private long convertToLong(String field) {
    return Long.parseLong(field.trim());
  }

  private BigInteger convertToBigInteger(String field) {
    return new BigInteger(field.trim());
  }

  private double convertToDouble(String field) {
    return Double.parseDouble(field.trim());
  }

  private float convertToFloat(String field) {
    return Float.parseFloat(field.trim());
  }

  private LocalDate convertToDate(String field) {
    try {
      long fieldVal = Long.parseLong(field);
      return Instant.ofEpochMilli(fieldVal)
          .atZone(ZoneOffset.systemDefault())
          .toLocalDateTime()
          .toLocalDate();
    } catch (NumberFormatException e) {
      // ignored
    }
    return localDateFormatter.parse(field).query(TemporalQueries.localDate());
  }

  private LocalTime convertToTime(String field) {
    try {
      long fieldVal = Long.parseLong(field);
      return LocalTime.ofSecondOfDay(fieldVal);
    } catch (NumberFormatException e) {
      // ignored
    }
    return localTimeFormatter.parse(field).query(TemporalQueries.localTime());
  }

  private LocalDateTime convertToTimestamp(String field) {
    try {
      long fieldVal = Long.parseLong(field);
      return Instant.ofEpochMilli(fieldVal)
          .atZone(ZoneOffset.systemDefault())
          .toLocalDateTime();
    } catch (NumberFormatException e) {
      // ignored
    }

    TemporalAccessor parse = localDateTimeFormatter.parse(field);
    LocalTime localTime = parse.query(TemporalQueries.localTime());
    LocalDate localDate = parse.query(TemporalQueries.localDate());

    return LocalDateTime.of(localDate, localTime);
  }

  private String convertToString(String field) {
    return field;
  }

  private byte[] convertToBytes(String field) {
    return field.getBytes();
  }

  private BigDecimal createDecimalConverter(String field) {
    return new BigDecimal(field);
  }

}
