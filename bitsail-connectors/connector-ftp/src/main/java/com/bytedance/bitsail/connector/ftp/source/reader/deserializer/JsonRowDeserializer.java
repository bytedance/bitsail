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
import com.bytedance.bitsail.common.util.FastJsonUtil;
import com.bytedance.bitsail.connector.ftp.error.FtpErrorCode;
import com.bytedance.bitsail.connector.ftp.option.FtpReaderOptions;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class JsonRowDeserializer implements ITextRowDeserializer {

  private final boolean isCaseInsensitive;
  private final int fieldSize;
  private final List<Function<JSONObject, Object>> converters;
  private boolean convertErrorColumnAsNull;
  private SerializerFeature[] serializerFeatures;

  public JsonRowDeserializer(BitSailConfiguration jobConf) {
    List<ColumnInfo> columnInfos = jobConf.get(FtpReaderOptions.COLUMNS);
    this.converters = columnInfos.stream().map(this::initWrappedConverter).collect(Collectors.toList());

    this.fieldSize = converters.size();

    this.isCaseInsensitive = jobConf.get(FtpReaderOptions.CASE_INSENSITIVE);

    this.serializerFeatures =
        FastJsonUtil.parseSerializerFeaturesFromConfig(jobConf.get(FtpReaderOptions.JSON_SERIALIZER_FEATURES)).toArray(new SerializerFeature[0]);
    this.convertErrorColumnAsNull = jobConf.get(FtpReaderOptions.CONVERT_ERROR_COLUMN_AS_NULL);
  }

  @Override
  public Row convert(String line) throws IOException {
    Row row = new Row(fieldSize);
    JSONObject jsonObj;
    if (this.isCaseInsensitive) {
      jsonObj = (JSONObject) convertJsonKeyToLowerCase(FastJsonUtil.parse(line));
    } else {
      jsonObj = (JSONObject) FastJsonUtil.parse(line);
    }
    for (int i = 0; i < fieldSize; ++i) {
      row.setField(i, converters.get(i).apply(jsonObj));
    }
    return row;
  }

  private Function<JSONObject, Object> initWrappedConverter(ColumnInfo columnInfo) {
    String columnName = columnInfo.getName();
    Function<JSONObject, Object> converter = initConverter(columnInfo);

    return jsonObject -> {
      if (jsonObject.get(columnName) == null) {
        return  null;
      } else {
        try {
          return converter.apply(jsonObject);
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

  private Function<JSONObject, Object> initConverter(ColumnInfo columnInfo) {
    String columnName = columnInfo.getName();
    String typeName = columnInfo.getType().trim().toUpperCase();

    switch (typeName) {
      case "BOOLEAN":
        return jsonObject -> jsonObject.getBoolean(columnName);
      case "TINYINT":
        return jsonObject -> jsonObject.getByteValue(columnName);
      case "SMALLINT":
        return jsonObject -> jsonObject.getShortValue(columnName);
      case "INT":
        return jsonObject -> jsonObject.getIntValue(columnName);
      case "BIGINT":
      case "LONG":
        return jsonObject -> jsonObject.getLongValue(columnName);
      case "DATE":
        return jsonObject -> jsonObject.getDate(columnName);
      case "TIMESTAMP":
        return jsonObject -> jsonObject.getTimestamp(columnName);
      case "FLOAT":
        return jsonObject -> jsonObject.getFloatValue(columnName);
      case "DOUBLE":
        return jsonObject -> jsonObject.getDouble(columnName);
      case "DECIMAL":
        return jsonObject -> jsonObject.getBigDecimal(columnName);
      case "CHAR":
      case "VARCHAR":
      case "STRING":
        return jsonObject -> {
          if (jsonObject.get(columnName) instanceof JSONObject) {
            return JSONObject.toJSONString(jsonObject.get(columnName), serializerFeatures);
          } else {
            return jsonObject.getString(columnName);
          }
        };
      case "STRING_UTF8":
      case "BINARY":
        return jsonObject -> jsonObject.getBytes(columnName);
      default:
        throw new BitSailException(FtpErrorCode.UNSUPPORTED_TYPE, "Type " + typeName + " is not supported");
    }
  }

  public Object convertJsonKeyToLowerCase(Object obj) {
    if (obj instanceof JSONObject) {
      JSONObject jsonObj = (JSONObject) obj;
      JSONObject out = new JSONObject();
      for (Object key : jsonObj.keySet()) {
        String keyString = key.toString().toLowerCase();
        Object valueObject = jsonObj.get(key);
        out.put(keyString, convertJsonKeyToLowerCase(valueObject));
      }
      return out;
    } else if (obj instanceof JSONArray) {
      JSONArray arrayObj = (JSONArray) obj;
      JSONArray out = new JSONArray();
      for (Object elementObject : arrayObj) {
        out.add(convertJsonKeyToLowerCase(elementObject));
      }
      return out;
    } else {
      return obj;
    }
  }
}