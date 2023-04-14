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

package com.bytedance.bitsail.connector.print.sink;

import com.bytedance.bitsail.base.connector.writer.v1.Writer;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.connector.print.sink.option.PrintWriterOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class PrintWriter implements Writer<Row, String, Integer> {
  private static final Logger LOG = LoggerFactory.getLogger(PrintWriter.class);

  private final int subTaskId;

  private final String[] fieldNames;
  private final boolean sampleWrite;
  private final int sampleLimit;

  private final AtomicInteger samplePrintCount;
  private final AtomicInteger printCount;
  private final List<String> commitPrint;

  private transient PrintStream stream = System.out;

  public PrintWriter(BitSailConfiguration writerConfiguration, Writer.Context<Integer> context) {
    this(writerConfiguration, context, 0);
  }

  public PrintWriter(BitSailConfiguration writerConfiguration, Writer.Context<Integer> context, int alreadyPrintCount) {

    this.sampleWrite = writerConfiguration.get(PrintWriterOptions.SAMPLE_WRITE);
    this.sampleLimit = writerConfiguration.get(PrintWriterOptions.SAMPLE_LIMIT);
    this.fieldNames = context.getRowTypeInfo().getFieldNames();

    printCount = new AtomicInteger(alreadyPrintCount);
    samplePrintCount = new AtomicInteger(0);
    this.commitPrint = new ArrayList<>();

    this.subTaskId = context.getIndexOfSubTaskId();
    LOG.info("Print sink writer {} is initialized.", subTaskId);
  }

  @Override
  public void write(Row element) {
    if (!this.sampleWrite) {
      this.stream.println(stringByRow(element));
    } else if (printCount.get() % this.sampleLimit == 0) {
      this.stream.println(stringByRow(element));
      samplePrintCount.incrementAndGet();
    }
    printCount.incrementAndGet();
  }

  private String stringByRow(Row element) {
    String[] fields = new String[element.getFields().length];
    for (int i = 0; i < element.getFields().length; ++i) {
      fields[i] = String.format("\"%s\":\"%s\"", fieldNames[i],
          element.getField(i) == null ? null : element.getField(i).toString());
    }
    return String.format("[%s]", String.join(",", fields));
  }

  @Override
  public void flush(boolean endOfInput) {
  }

  @Override
  public List<String> prepareCommit() {
    if (this.sampleWrite) {
      this.commitPrint.add(String.format("PrintSink-%d Input number records:%d, sample print %d records",
          this.subTaskId, this.printCount.get(), this.samplePrintCount.get()));
    } else {
      this.commitPrint.add(String.format("PrintSink-%d print %d records", this.subTaskId, this.printCount.get()));
    }
    return this.commitPrint;
  }

  @Override
  public List<Integer> snapshotState(long checkpointId) {
    return Collections.singletonList(printCount.get());
  }
}
