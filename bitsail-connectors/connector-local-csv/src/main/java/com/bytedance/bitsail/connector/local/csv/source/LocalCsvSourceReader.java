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

package com.bytedance.bitsail.connector.local.csv.source;

import com.bytedance.bitsail.base.connector.reader.v1.SourcePipeline;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.component.format.csv.CsvDeserializationSchema;
import com.bytedance.bitsail.connector.base.source.SimpleSourceReaderBase;
import com.bytedance.bitsail.connector.local.csv.option.LocalCsvReaderOptions;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicLong;

public class LocalCsvSourceReader extends SimpleSourceReaderBase<Row> {
  private final transient int totalCount;
  private final transient AtomicLong counter;
  private final String filePath;
  private final BufferedReader bufferedReader;
  private final CsvDeserializationSchema deserializationSchema;

  LocalCsvSourceReader(BitSailConfiguration readerConfiguration, Context context) {
    this.filePath = readerConfiguration.get(LocalCsvReaderOptions.FILE_PATH);
    this.bufferedReader = loadCsvFile();
    this.counter = new AtomicLong();
    this.totalCount = lineCount();
    this.deserializationSchema = new CsvDeserializationSchema(
      readerConfiguration,
      context.getTypeInfos(),
      context.getFieldNames()
    );
  }

  private BufferedReader loadCsvFile() {
    Path path = Paths.get(this.filePath);
    if (!Files.exists(path)) {
      throw new RuntimeException(new FileNotFoundException(
        String.format("File %s does not exits!", this.filePath)
      ));
    }

    BufferedReader bufferedReader;
    try {
      bufferedReader = Files.newBufferedReader(path);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return bufferedReader;
  }

  private int lineCount() {
    int lineNumber;
    try (BufferedReader bufferedReader = new BufferedReader(new FileReader(this.filePath))) {
      LineNumberReader lineNumberReader = new LineNumberReader(bufferedReader);
      lineNumberReader.skip(Long.MAX_VALUE);
      lineNumber = lineNumberReader.getLineNumber() + 1;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return lineNumber;
  }

  @Override
  public void pollNext(SourcePipeline<Row> pipeline) throws Exception {
    String line = this.bufferedReader.readLine();
    Row row = deserializationSchema.deserialize(line.getBytes());
    pipeline.output(row);

    if (counter.get() == totalCount) {
      this.bufferedReader.close();
    }
  }

  @Override
  public boolean hasMoreElements() {
    return counter.incrementAndGet() <= totalCount;
  }
}
