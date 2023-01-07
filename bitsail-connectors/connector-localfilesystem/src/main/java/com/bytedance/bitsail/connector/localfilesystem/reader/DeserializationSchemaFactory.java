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

package com.bytedance.bitsail.connector.localfilesystem.reader;

import com.bytedance.bitsail.base.connector.reader.v1.SourceReader;
import com.bytedance.bitsail.base.format.DeserializationSchema;
import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.component.format.csv.CsvDeserializationSchema;
import com.bytedance.bitsail.component.format.json.JsonDeserializationSchema;
import com.bytedance.bitsail.connector.localfilesystem.core.config.FileSystemConfig;
import com.bytedance.bitsail.connector.localfilesystem.error.FileSystemErrorCode;

public class DeserializationSchemaFactory {
  public static DeserializationSchema<byte[], Row> createDeserializationSchema(BitSailConfiguration jobConf, SourceReader.Context context,
                                                                               FileSystemConfig fileSystemConfig) {
    if (fileSystemConfig.getContentType() == FileSystemConfig.ContentType.CSV) {
      return new CsvDeserializationSchema(
          jobConf,
          context.getTypeInfos(),
          context.getFieldNames());
    } else if (fileSystemConfig.getContentType() == FileSystemConfig.ContentType.JSON) {
      return new JsonDeserializationSchema(
          jobConf,
          context.getTypeInfos(),
          context.getFieldNames());
    } else {
      throw BitSailException.asBitSailException(FileSystemErrorCode.UNSUPPORTED_CONTENT_TYPE,
          "Content type only supports CSV and JSON");
    }
  }
}
