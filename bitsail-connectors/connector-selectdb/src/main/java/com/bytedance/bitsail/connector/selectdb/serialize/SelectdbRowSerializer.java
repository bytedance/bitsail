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

package com.bytedance.bitsail.connector.selectdb.serialize;

import com.bytedance.bitsail.common.model.ColumnInfo;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.common.row.RowKind;
import com.bytedance.bitsail.connector.selectdb.config.SelectdbOptions;
import com.bytedance.bitsail.connector.selectdb.constant.SelectdbConstants;
import com.bytedance.bitsail.connector.selectdb.serialize.converter.SelectdbRowConverter;
import com.bytedance.bitsail.connector.selectdb.typeinfo.SelectdbDataType;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

public class SelectdbRowSerializer implements Serializable {
  protected String[] fieldNames;
  protected SelectdbDataType[] dataTypes;
  protected SelectdbOptions.LOAD_CONTENT_TYPE type;
  protected ObjectMapper objectMapper;
  protected String fieldDelimiter;
  protected boolean enableDelete;
  protected SelectdbRowConverter rowConverter;

  protected enum DELETED_FLAG {
    // not delete is 0
    NOT_DELETED("0"),
    DELETED("1");

    final String value;

    /**
     * delete flag
     */
    DELETED_FLAG(String value) {
      this.value = value;
    }

    String getValue() {
      return this.value;
    }
  }

  @VisibleForTesting
  public SelectdbRowSerializer() {
  }

  public SelectdbRowSerializer(List<ColumnInfo> columnInfoList, SelectdbOptions.LOAD_CONTENT_TYPE type,
                               String fieldDelimiter, boolean enableDelete) {
    this.fieldNames = new String[columnInfoList.size()];
    this.dataTypes = new SelectdbDataType[columnInfoList.size()];
    for (int i = 0; i < columnInfoList.size(); i++) {
      this.fieldNames[i] = columnInfoList.get(i).getName();
      this.dataTypes[i] = SelectdbDataType.valueOf(columnInfoList.get(i).getType().toUpperCase());
    }
    this.type = type;
    this.fieldDelimiter = fieldDelimiter;
    this.enableDelete = enableDelete;
    if (SelectdbOptions.LOAD_CONTENT_TYPE.JSON.equals(type)) {
      objectMapper = new ObjectMapper();
    }
    this.rowConverter = new SelectdbRowConverter(dataTypes);
  }

  public String serialize(Row record) throws IOException {
    int maxIndex = Math.min(record.getArity(), fieldNames.length);
    String valString;
    if (SelectdbOptions.LOAD_CONTENT_TYPE.JSON.equals(type)) {
      valString = buildJsonString(record, maxIndex);
    } else if (SelectdbOptions.LOAD_CONTENT_TYPE.CSV.equals(type)) {
      valString = buildCSVString(record, maxIndex);
    } else {
      throw new IllegalArgumentException("The type " + type + " is not supported!");
    }
    return valString;
  }

  public String buildJsonString(Row record, int maxIndex) throws IOException {
    int fieldIndex = 0;
    Map<String, String> valueMap = new HashMap<>();
    while (fieldIndex < maxIndex) {
      Object field = rowConverter.convertExternal(record, fieldIndex);
      String value = field != null ? field.toString() : null;
      valueMap.put(fieldNames[fieldIndex], value);
      fieldIndex++;
    }
    if (enableDelete) {
      valueMap.put(SelectdbConstants.SELECTDB_DELETE_SIGN, parseDeleteSign(record.getKind()).getValue());
    }
    return objectMapper.writeValueAsString(valueMap);
  }

  public String buildCSVString(Row record, int maxIndex) {
    int fieldIndex = 0;
    StringJoiner joiner = new StringJoiner(fieldDelimiter);
    while (fieldIndex < maxIndex) {
      Object field = rowConverter.convertExternal(record, fieldIndex);
      String value = field != null ? field.toString() : SelectdbConstants.NULL_VALUE;
      joiner.add(value);
      fieldIndex++;
    }
    if (enableDelete) {
      joiner.add(parseDeleteSign(record.getKind()).getValue());
    }
    return joiner.toString();
  }

  public DELETED_FLAG parseDeleteSign(RowKind rowKind) {
    if (RowKind.INSERT.equals(rowKind) || RowKind.UPDATE_AFTER.equals(rowKind)) {
      return DELETED_FLAG.NOT_DELETED;
    } else if (RowKind.DELETE.equals(rowKind) || RowKind.UPDATE_BEFORE.equals(rowKind)) {
      return DELETED_FLAG.DELETED;
    } else {
      throw new IllegalArgumentException("Unrecognized row kind:" + rowKind.toString());
    }
  }
}

