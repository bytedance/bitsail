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

package com.bytedance.bitsail.connector.legacy.streamingfile.common.parser;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.model.ColumnInfo;
import com.bytedance.bitsail.connector.legacy.streamingfile.common.validator.StreamingFileSystemValidator;
import com.bytedance.bitsail.dump.datasink.file.parser.BytesParser;
import com.bytedance.bitsail.dump.datasink.file.parser.JsonBytesParser;
import com.bytedance.bitsail.dump.datasink.file.parser.PbBytesParser;

import java.util.List;

/**
 * @class: BytesParseFactory
 * @desc:
 **/
public class BytesParseFactory {
  public static BytesParser initBytesParser(BitSailConfiguration jobConf,
                                            String dumpFormatType,
                                            List<ColumnInfo> columnInfos) throws Exception {
    switch (dumpFormatType) {
      case StreamingFileSystemValidator.HDFS_DUMP_TYPE_JSON:
        return new JsonBytesParser(jobConf, columnInfos);
      case StreamingFileSystemValidator.HDFS_DUMP_TYPE_PB:
        return new PbBytesParser(jobConf, columnInfos);
      default:
        throw new RuntimeException("Unsupported dump type: " + dumpFormatType);
    }
  }
}
