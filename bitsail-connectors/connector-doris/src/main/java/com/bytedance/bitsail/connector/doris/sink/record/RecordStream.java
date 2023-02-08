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

package com.bytedance.bitsail.connector.doris.sink.record;

import java.io.IOException;
import java.io.InputStream;

/**
 * Record Stream for writing record.
 */
public class RecordStream extends InputStream {
  private final RecordBuffer recordBuffer;

  @Override
  public int read() throws IOException {
    return 0;
  }

  @Override
  public int read(byte[] buff) throws IOException {
    try {
      return recordBuffer.read(buff);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public RecordStream(int bufferSize, int bufferCount) {
    this.recordBuffer = new RecordBuffer(bufferSize, bufferCount);
  }

  public void startInput() {
    recordBuffer.startBufferData();
  }

  public void endInput() throws IOException {
    recordBuffer.stopBufferData();
  }

  public void write(byte[] buff) throws IOException {
    try {
      recordBuffer.write(buff);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}