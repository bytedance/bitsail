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

package com.bytedance.bitsail.connector.localfilesystem.reader;

import com.bytedance.bitsail.base.connector.reader.v1.SourcePipeline;
import com.bytedance.bitsail.base.format.DeserializationSchema;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.connector.base.source.SimpleSourceReaderBase;
import com.bytedance.bitsail.connector.localfilesystem.core.config.LocalFileSystemConfig;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class LocalFileSystemSourceReader extends SimpleSourceReaderBase<Row> {
  private final LocalFileSystemConfig localFileSystemConfig;
  private final String filePath;
  private final BufferedReader bufferedReader;
  private String line;
  private final transient DeserializationSchema<byte[], Row> deserializationSchema;

  public LocalFileSystemSourceReader(BitSailConfiguration jobConf, Context context) {
    this.localFileSystemConfig = new LocalFileSystemConfig(jobConf);
    this.filePath = localFileSystemConfig.getFilePath();
    this.bufferedReader = loadLocalFile();
    this.deserializationSchema = DeserializationSchemaFactory.createDeserializationSchema(
        jobConf,
        context,
        localFileSystemConfig
    );
  }

  private BufferedReader loadLocalFile() {
    Path path = Paths.get(filePath);
    if (!Files.exists(path)) {
      throw new RuntimeException(new FileNotFoundException(
        String.format("File %s does not exits!", filePath)
      ));
    }

    BufferedReader bufferedReader;
    try {
      bufferedReader = Files.newBufferedReader(path);
      if (localFileSystemConfig.getSkipFirstLine()) {
        bufferedReader.readLine();
      }
      line = bufferedReader.readLine();
      if (line == null) {
        bufferedReader.close();
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return bufferedReader;
  }

  @Override
  public void pollNext(SourcePipeline<Row> pipeline) throws Exception {
    synchronized (this) {
      // Avoid NPE
      if (line == null) {
        return;
      }

      Row row = deserializationSchema.deserialize(line.getBytes());
      pipeline.output(row);
      line = bufferedReader.readLine();
      if (line == null) {
        bufferedReader.close();
      }
    }
  }

  @Override
  public boolean hasMoreElements() {
    synchronized (this) {
      return line != null;
    }
  }
}
