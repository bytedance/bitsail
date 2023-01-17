/*
 * Copyright 2022 Bytedance Ltd. and/or its affiliates.
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

package com.bytedance.bitsail.connector.elasticsearch.source.reader.deserializer;

import com.bytedance.bitsail.base.format.DeserializationSchema;
import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.exception.CommonErrorCode;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.common.typeinfo.BasicTypeInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfos;
import com.bytedance.bitsail.connector.elasticsearch.error.ElasticsearchErrorCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class ElasticsearchRowDeserializer implements DeserializationSchema<Map<String, Object>, Row> {

  private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchRowDeserializer.class);
  private final TypeInfo<?>[] typeInfos;

  private final String[] fieldNames;

  private final int fieldSize;

  private final BitSailConfiguration deserializerConf;

  public ElasticsearchRowDeserializer(TypeInfo<?>[] typeInfos, String[] fieldNames, BitSailConfiguration deserializerConf) {
    this.typeInfos = typeInfos;
    this.fieldNames = fieldNames;
    this.deserializerConf = deserializerConf;
    this.fieldSize = this.typeInfos.length;
  }

  @Override
  public Row deserialize(Map<String, Object> message) {
    Row row = new Row(this.fieldSize);
    for (int i = 0; i < this.fieldSize; i++) {
      String fieldName = this.fieldNames[i];
      Object value = message.getOrDefault(fieldName, null);
      if (Objects.nonNull(value)) {
        TypeInfo<?> typeInfo = this.typeInfos[i];
        try {
          row.setField(i, convert(typeInfo, value.toString()));
        } catch (ParseException e) {
          LOG.error("Parse value {} with type {} failed.", value, typeInfo);
          throw new RuntimeException(e);
        }
      }
    }
    return row;
  }

  @Override
  public boolean isEndOfStream(Row nextElement) {
    return false;
  }

  private Object convert(TypeInfo<?> typeInfo, String value) throws ParseException {
    if (!(typeInfo instanceof BasicTypeInfo)) {
      throw BitSailException.asBitSailException(CommonErrorCode.UNSUPPORTED_COLUMN_TYPE, typeInfo.getTypeClass().getName() + " is not supported yet.");
    }
    Class<?> curClass = typeInfo.getTypeClass();
    if (TypeInfos.BYTE_TYPE_INFO.getTypeClass() == curClass) {
      return Boolean.parseBoolean(value);
    }
    if (TypeInfos.SHORT_TYPE_INFO.getTypeClass() == curClass) {
      return Short.parseShort(value);
    }
    if (TypeInfos.INT_TYPE_INFO.getTypeClass() == curClass) {
      return Integer.parseInt(value);
    }
    if (TypeInfos.LONG_TYPE_INFO.getTypeClass() == curClass) {
      return Long.parseLong(value);
    }
    if (TypeInfos.FLOAT_TYPE_INFO.getTypeClass() == curClass) {
      return Float.parseFloat(value);
    }
    if (TypeInfos.DOUBLE_TYPE_INFO.getTypeClass() == curClass) {
      return Double.parseDouble(value);
    }
    if (TypeInfos.BIG_INTEGER_TYPE_INFO.getTypeClass() == curClass) {
      return new BigInteger(value);
    }
    if (TypeInfos.BIG_DECIMAL_TYPE_INFO.getTypeClass() == curClass) {
      return new BigDecimal(value);
    }
    if (TypeInfos.STRING_TYPE_INFO.getTypeClass() == curClass) {
      return value;
    }
    if (TypeInfos.BOOLEAN_TYPE_INFO.getTypeClass() == curClass) {
      return Boolean.parseBoolean(value);
    }
    if (TypeInfos.LOCAL_DATE_TYPE_INFO.getTypeClass() == curClass) {
      return LocalDate.parse(value);
    }
    if (TypeInfos.LOCAL_TIME_TYPE_INFO.getTypeClass() == curClass) {
      return LocalTime.parse(value);
    }
    if (TypeInfos.LOCAL_DATE_TIME_TYPE_INFO.getTypeClass() == curClass) {
      return LocalDateTime.parse(value);
    }
    if (TypeInfos.SQL_DATE_TYPE_INFO.getTypeClass() == curClass) {
      String format = determineDateFormat(value);
      SimpleDateFormat simpleDateFormat = new SimpleDateFormat(format);
      return simpleDateFormat.parse(value);
    }
    if (TypeInfos.SQL_TIME_TYPE_INFO.getTypeClass() == curClass) {
      return Timestamp.parse(value);
    }
    if (TypeInfos.SQL_TIMESTAMP_TYPE_INFO.getTypeClass() == curClass) {
      return Timestamp.parse(value);
    }
    if (TypeInfos.VOID_TYPE_INFO.getTypeClass() == curClass) {
      return null;
    }
    throw new UnsupportedOperationException("Unsupported data type: " + typeInfo);
  }

  /**
   * Determine SimpleDateFormat pattern matching with the given date string. Returns null if
   * format is unknown. You can simply extend DateUtil with more formats if needed.
   * <a href="https://stackoverflow.com/questions/3389348/parse-any-date-in-java">...</a>
   *
   * @param dateString The date string to determine the SimpleDateFormat pattern for.
   * @return The matching SimpleDateFormat pattern, or null if format is unknown.
   * @see SimpleDateFormat
   */
  public static String determineDateFormat(String dateString) {
    for (String regexp : DATE_FORMAT_REGEXPS.keySet()) {
      if (dateString.toLowerCase().matches(regexp)) {
        return DATE_FORMAT_REGEXPS.get(regexp);
      }
    }
    // Unknown format.
    throw BitSailException.asBitSailException(
        ElasticsearchErrorCode.DESERIALIZE_FAILED,
        "Format not recognized."
    );
  }

  private static final Map<String, String> DATE_FORMAT_REGEXPS = new HashMap<String, String>() {{
      put("^\\d{8}$", "yyyyMMdd");
      put("^\\d{1,2}-\\d{1,2}-\\d{4}$", "dd-MM-yyyy");
      put("^\\d{4}-\\d{1,2}-\\d{1,2}$", "yyyy-MM-dd");
      put("^\\d{1,2}/\\d{1,2}/\\d{4}$", "MM/dd/yyyy");
      put("^\\d{4}/\\d{1,2}/\\d{1,2}$", "yyyy/MM/dd");
      put("^\\d{1,2}\\s[a-z]{3}\\s\\d{4}$", "dd MMM yyyy");
      put("^\\d{1,2}\\s[a-z]{4,}\\s\\d{4}$", "dd MMMM yyyy");
      put("^\\d{12}$", "yyyyMMddHHmm");
      put("^\\d{8}\\s\\d{4}$", "yyyyMMdd HHmm");
      put("^\\d{1,2}-\\d{1,2}-\\d{4}\\s\\d{1,2}:\\d{2}$", "dd-MM-yyyy HH:mm");
      put("^\\d{4}-\\d{1,2}-\\d{1,2}\\s\\d{1,2}:\\d{2}$", "yyyy-MM-dd HH:mm");
      put("^\\d{1,2}/\\d{1,2}/\\d{4}\\s\\d{1,2}:\\d{2}$", "MM/dd/yyyy HH:mm");
      put("^\\d{4}/\\d{1,2}/\\d{1,2}\\s\\d{1,2}:\\d{2}$", "yyyy/MM/dd HH:mm");
      put("^\\d{1,2}\\s[a-z]{3}\\s\\d{4}\\s\\d{1,2}:\\d{2}$", "dd MMM yyyy HH:mm");
      put("^\\d{1,2}\\s[a-z]{4,}\\s\\d{4}\\s\\d{1,2}:\\d{2}$", "dd MMMM yyyy HH:mm");
      put("^\\d{14}$", "yyyyMMddHHmmss");
      put("^\\d{8}\\s\\d{6}$", "yyyyMMdd HHmmss");
      put("^\\d{1,2}-\\d{1,2}-\\d{4}\\s\\d{1,2}:\\d{2}:\\d{2}$", "dd-MM-yyyy HH:mm:ss");
      put("^\\d{4}-\\d{1,2}-\\d{1,2}\\s\\d{1,2}:\\d{2}:\\d{2}$", "yyyy-MM-dd HH:mm:ss");
      put("^\\d{1,2}/\\d{1,2}/\\d{4}\\s\\d{1,2}:\\d{2}:\\d{2}$", "MM/dd/yyyy HH:mm:ss");
      put("^\\d{4}/\\d{1,2}/\\d{1,2}\\s\\d{1,2}:\\d{2}:\\d{2}$", "yyyy/MM/dd HH:mm:ss");
      put("^\\d{1,2}\\s[a-z]{3}\\s\\d{4}\\s\\d{1,2}:\\d{2}:\\d{2}$", "dd MMM yyyy HH:mm:ss");
      put("^\\d{1,2}\\s[a-z]{4,}\\s\\d{4}\\s\\d{1,2}:\\d{2}:\\d{2}$", "dd MMMM yyyy HH:mm:ss");
    }};
}
