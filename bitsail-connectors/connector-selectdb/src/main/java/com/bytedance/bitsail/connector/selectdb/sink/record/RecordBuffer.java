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

package com.bytedance.bitsail.connector.selectdb.sink.record;

import com.bytedance.bitsail.common.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Channel of record stream and HTTP data stream.
 */
public class RecordBuffer {
  private static final Logger LOG = LoggerFactory.getLogger(RecordBuffer.class);
  BlockingQueue<ByteBuffer> writeQueue;
  BlockingQueue<ByteBuffer> readQueue;
  int bufferCapacity;
  int queueSize;
  ByteBuffer currentWriteBuffer;
  ByteBuffer currentReadBuffer;

  public RecordBuffer(int capacity, int queueSize) {
    LOG.info("init RecordBuffer capacity {}, count {}", capacity, queueSize);
    Preconditions.checkState(capacity > 0);
    Preconditions.checkState(queueSize > 1);
    this.writeQueue = new ArrayBlockingQueue<>(queueSize);
    for (int index = 0; index < queueSize; index++) {
      this.writeQueue.add(ByteBuffer.allocate(capacity));
    }
    readQueue = new LinkedBlockingDeque<>();
    this.bufferCapacity = capacity;
    this.queueSize = queueSize;
  }

  public void startBufferData() {
    LOG.info("start buffer data, read queue size {}, write queue size {}", readQueue.size(), writeQueue.size());
    Preconditions.checkState(readQueue.size() == 0);
    Preconditions.checkState(writeQueue.size() == queueSize);
    for (ByteBuffer byteBuffer : writeQueue) {
      Preconditions.checkState(byteBuffer.position() == 0);
      Preconditions.checkState(byteBuffer.remaining() == bufferCapacity);
    }
  }

  public void stopBufferData() throws IOException {
    try {
      // add Empty buffer as finish flag.
      boolean isEmpty = false;
      if (currentWriteBuffer != null) {
        currentWriteBuffer.flip();
        // check if the current write buffer is empty.
        isEmpty = currentWriteBuffer.limit() == 0;
        readQueue.put(currentWriteBuffer);
        currentWriteBuffer = null;
      }
      if (!isEmpty) {
        ByteBuffer byteBuffer = writeQueue.take();
        byteBuffer.flip();
        Preconditions.checkState(byteBuffer.limit() == 0);
        readQueue.put(byteBuffer);
      }
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  public void write(byte[] buf) throws InterruptedException {
    int writePos = 0;
    do {
      if (currentWriteBuffer == null) {
        currentWriteBuffer = writeQueue.take();
      }
      int available = currentWriteBuffer.remaining();
      int minWrite = Math.min(available, buf.length - writePos);
      currentWriteBuffer.put(buf, writePos, minWrite);
      writePos += minWrite;
      if (currentWriteBuffer.remaining() == 0) {
        currentWriteBuffer.flip();
        readQueue.put(currentWriteBuffer);
        currentWriteBuffer = null;
      }
    } while (writePos != buf.length);
  }

  public int read(byte[] buf) throws InterruptedException {
    if (currentReadBuffer == null) {
      currentReadBuffer = readQueue.take();
    }
    // add empty buffer as end flag
    if (currentReadBuffer.limit() == 0) {
      recycleBuffer(currentReadBuffer);
      currentReadBuffer = null;
      Preconditions.checkState(readQueue.size() == 0);
      return -1;
    }
    int available = currentReadBuffer.remaining();
    int minRead = Math.min(available, buf.length);
    currentReadBuffer.get(buf, 0, minRead);
    if (currentReadBuffer.remaining() == 0) {
      recycleBuffer(currentReadBuffer);
      currentReadBuffer = null;
    }
    return minRead;
  }

  private void recycleBuffer(ByteBuffer buffer) throws InterruptedException {
    buffer.clear();
    writeQueue.put(buffer);
  }

  public int getWriteQueueSize() {
    return writeQueue.size();
  }

  public int getReadQueueSize() {
    return readQueue.size();
  }
}
