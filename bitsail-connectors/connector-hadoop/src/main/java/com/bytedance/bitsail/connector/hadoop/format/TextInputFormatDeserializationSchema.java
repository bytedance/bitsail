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

package com.bytedance.bitsail.connector.hadoop.format;

import com.bytedance.bitsail.base.enumerate.ContentType;
import com.bytedance.bitsail.base.format.DeserializationSchema;
import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.common.typeinfo.TypeInfo;
import com.bytedance.bitsail.component.format.csv.CsvDeserializationSchema;
import com.bytedance.bitsail.component.format.json.JsonDeserializationSchema;
import com.bytedance.bitsail.connector.hadoop.error.HadoopErrorCode;
import com.bytedance.bitsail.connector.hadoop.option.HadoopReaderOptions;

import org.apache.hadoop.io.Writable;

public class TextInputFormatDeserializationSchema implements DeserializationSchema<Writable, Row> {
  private final BitSailConfiguration deserializationConfiguration;
  private final TypeInfo<?>[] typeInfos;
  private final String[] fieldNames;
  private final DeserializationSchema<byte[], Row> deserializationSchema;

  public TextInputFormatDeserializationSchema(BitSailConfiguration deserializationConfiguration,
                                               TypeInfo<?>[] typeInfos,
                                               String[] fieldNames) {
    this.deserializationConfiguration = deserializationConfiguration;
    this.typeInfos = typeInfos;
    this.fieldNames = fieldNames;

    ContentType contentType = ContentType.valueOf(
        deserializationConfiguration.getNecessaryOption(HadoopReaderOptions.CONTENT_TYPE, HadoopErrorCode.REQUIRED_VALUE).toUpperCase());
    switch (contentType) {
      case CSV:
        this.deserializationSchema =
            new CsvDeserializationSchema(deserializationConfiguration, typeInfos, fieldNames);
        break;
      case JSON:
        this.deserializationSchema =
            new JsonDeserializationSchema(deserializationConfiguration, typeInfos, fieldNames);
        break;
      default:
        throw BitSailException.asBitSailException(HadoopErrorCode.UNSUPPORTED_ENCODING, "unsupported parser type: " + contentType);
    }
  }

  @Override
  public Row deserialize(Writable message) {
    return deserializationSchema.deserialize((message.toString()).getBytes());
  }

  @Override
  public boolean isEndOfStream(Row nextElement) {
    return false;
  }
}
