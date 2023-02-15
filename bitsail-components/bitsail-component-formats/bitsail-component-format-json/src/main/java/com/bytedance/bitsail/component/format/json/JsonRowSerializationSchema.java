/*
 * Copyright 2022-2023 Bytedance Ltd. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.bytedance.bitsail.component.format.json;

import com.bytedance.bitsail.base.format.SerializationSchema;
import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.common.typeinfo.RowTypeInfo;
import com.bytedance.bitsail.component.format.json.error.JsonFormatErrorCode;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Serialization schema of BitSail Row into Json bytes.
 */
public class JsonRowSerializationSchema implements SerializationSchema<Row> {
  private static final long serialVersionUID = 1L;

  private final RowToJsonConverter converter;

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public JsonRowSerializationSchema(BitSailConfiguration deserializationConfiguration,
                                    RowTypeInfo rowTypeInfo) {
    this.converter = new RowToJsonConverter(rowTypeInfo);
  }

  @Override
  public byte[] serialize(Row row) {
    try {
      return OBJECT_MAPPER.writeValueAsBytes(this.converter.convert(row));
    } catch (Exception e) {
      throw BitSailException.asBitSailException(JsonFormatErrorCode.JSON_FORMAT_COVERT_FAILED, e);
    }
  }
}
