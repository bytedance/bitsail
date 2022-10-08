/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bytedance.bitsail.connector.print.sink;

import com.bytedance.bitsail.base.connector.writer.v1.Writer;
import com.bytedance.bitsail.base.connector.writer.v1.WriterGenerator;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.model.ColumnInfo;
import com.bytedance.bitsail.common.option.WriterOptions;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.common.type.BaseEngineTypeInfoConverter;
import com.bytedance.bitsail.common.type.SimpleTypeInfoConverter;
import com.bytedance.bitsail.connector.print.sink.option.PrintWriterOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

public class PrintWriterGenerator implements WriterGenerator<Row, String, Integer> {
  private static final Logger LOG = LoggerFactory.getLogger(PrintWriterGenerator.class);

  private List<String> fieldNames;
  private int batchSize;

  @Override
  public String getWriterName() {
    return "print_writer";
  }

  @Override
  public void configure(BitSailConfiguration commonConfiguration, BitSailConfiguration writerConfiguration) {
    this.batchSize = writerConfiguration.get(PrintWriterOptions.BATCH_SIZE);
    this.fieldNames = writerConfiguration.get(WriterOptions.BaseWriterOptions.COLUMNS)
        .stream()
        .map(ColumnInfo::getName)
        .collect(Collectors.toList());
  }

  @Override
  public Writer<Row, String, Integer> createWriter(BitSailConfiguration writerConfiguration, Writer.Context context) {
    return new PrintWriter(batchSize, fieldNames);
  }

  @Override
  public Writer<Row, String, Integer> restoreWriter(BitSailConfiguration writerConfiguration,
                                                    List<Integer> writerStates,
                                                    Writer.Context context) {
    LOG.info("writerStatesList: {}", writerStates);
    return new PrintWriter(batchSize, fieldNames, writerStates.get(0));
  }

  @Override
  public BaseEngineTypeInfoConverter createTypeInfoConverter() {
    return new SimpleTypeInfoConverter("bitsail");
  }
}
