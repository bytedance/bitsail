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

package com.bytedance.bitsail.connector.print.sink;

import com.bytedance.bitsail.base.connector.writer.v1.Writer;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.common.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class PrintWriter implements Writer<Row, String, Integer> {
  private static final Logger LOG = LoggerFactory.getLogger(PrintWriter.class);

  private final int batchSize;
  private final List<String> fieldNames;
  private final boolean sampleWrite;
  private final int sampleLimit;

  private final List<String> writeBuffer;
  private final List<String> commitBuffer;

  private final AtomicInteger printCount;

  private transient PrintStream stream = System.out;

  public PrintWriter(int batchSize, List<String> fieldNames, boolean sampleWrite, int sampleLimit) {
    this(batchSize, fieldNames, sampleWrite, sampleLimit, 0);
  }

  public PrintWriter(int batchSize, List<String> fieldNames, boolean sampleWrite, int sampleLimit, int alreadyPrintCount) {
    Preconditions.checkState(batchSize > 0, "batch size must be larger than 0");
    this.batchSize = batchSize;
    this.fieldNames = fieldNames;
    this.sampleWrite = sampleWrite;
    this.sampleLimit = sampleLimit;
    this.writeBuffer = new ArrayList<>(batchSize);
    this.commitBuffer = new ArrayList<>(batchSize);
    printCount = new AtomicInteger(alreadyPrintCount);
  }

  @Override
  public void write(Row element) {
    if (!this.sampleWrite || printCount.get() % this.sampleLimit == 0) {
      String[] fields = new String[element.getFields().length];
      for (int i = 0; i < element.getFields().length; ++i) {
        fields[i] = String.format("\"%s\":\"%s\"", fieldNames.get(i), element.getField(i).toString());
      }

      writeBuffer.add("[" + String.join(",", fields) + "]");
      if (writeBuffer.size() == batchSize) {
        this.flush(false);
      }
    }
    printCount.incrementAndGet();
  }

  @Override
  public void flush(boolean endOfInput) {
    commitBuffer.addAll(writeBuffer);

    writeBuffer.forEach(
        element -> this.stream.println(element)
    );

    writeBuffer.clear();
    if (endOfInput) {
      LOG.info("all records are sent to commit buffer.");
    }
  }

  @Override
  public List<String> prepareCommit() {
    return commitBuffer;
  }

  @Override
  public List<Integer> snapshotState(long checkpointId) {
    return Collections.singletonList(printCount.get());
  }
}
