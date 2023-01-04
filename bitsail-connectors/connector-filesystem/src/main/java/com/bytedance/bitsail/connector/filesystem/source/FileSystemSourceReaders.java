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

package com.bytedance.bitsail.connector.filesystem.source;

import com.bytedance.bitsail.base.connector.reader.v1.SourceReader;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.connector.base.source.SimpleSourceReaderBase;
import com.bytedance.bitsail.connector.filesystem.option.FileSystemReaderOptions;
import com.bytedance.bitsail.connector.filesystem.source.reader.CsvSourceReader;

import java.nio.file.FileSystemException;

public class FileSystemSourceReaders {
  private FileSystemSourceReaders() {}

  public static SimpleSourceReaderBase<Row> createReader(BitSailConfiguration readerConfiguration,
                                                         SourceReader.Context readerContext) {
    String format = readerConfiguration.get(FileSystemReaderOptions.FORMAT);
    SimpleSourceReaderBase<Row> simpleSourceReaderBase = null;
    if (format.equals("CSV")) {
      simpleSourceReaderBase = new CsvSourceReader(readerConfiguration, readerContext);
    } else {
      throw new RuntimeException(
          new FileSystemException(
              String.format("File format of %s does not support!", format)
          )
      );
    }
    return simpleSourceReaderBase;
  }

}
