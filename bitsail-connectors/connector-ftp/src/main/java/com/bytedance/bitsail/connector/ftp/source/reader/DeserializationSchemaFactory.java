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

package com.bytedance.bitsail.connector.ftp.source.reader;

import com.bytedance.bitsail.base.connector.reader.v1.SourceReader;
import com.bytedance.bitsail.base.format.DeserializationSchema;
import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.component.format.csv.CsvDeserializationSchema;
import com.bytedance.bitsail.component.format.json.JsonRowDeserializationSchema;
import com.bytedance.bitsail.connector.ftp.core.config.FtpConfig;
import com.bytedance.bitsail.connector.ftp.error.FtpErrorCode;

public class DeserializationSchemaFactory {
  public static DeserializationSchema createDeserializationSchema(BitSailConfiguration jobConf, FtpConfig ftpConfig, SourceReader.Context context) {
    if (ftpConfig.getContentType() == FtpConfig.ContentType.CSV) {
      return new CsvDeserializationSchema(
          jobConf,
          context.getRowTypeInfo());
    } else if (ftpConfig.getContentType() == FtpConfig.ContentType.JSON) {
      return new JsonRowDeserializationSchema(
          jobConf,
          context.getRowTypeInfo());
    } else {
      throw BitSailException.asBitSailException(FtpErrorCode.CONTENT_TYPE_NOT_SUPPORTED,
          "Content type only supports JSON and CSV");
    }
  }
}
