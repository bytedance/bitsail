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

import com.bytedance.bitsail.base.connector.writer.v1.Sink;
import com.bytedance.bitsail.base.connector.writer.v1.Writer;
import com.bytedance.bitsail.base.connector.writer.v1.WriterCommitter;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.common.type.BitSailTypeInfoConverter;
import com.bytedance.bitsail.common.type.TypeInfoConverter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class PrintSink implements Sink<Row, String, Integer> {
  private static final Logger LOG = LoggerFactory.getLogger(PrintSink.class);

  private BitSailConfiguration writerConfiguration;

  @Override
  public String getWriterName() {
    return "print";
  }

  @Override
  public void configure(BitSailConfiguration commonConfiguration, BitSailConfiguration writerConfiguration) {
    this.writerConfiguration = writerConfiguration;
  }

  @Override
  public Writer<Row, String, Integer> createWriter(Writer.Context<Integer> context) {
    return new PrintWriter(writerConfiguration, context);
  }

  @Override
  public TypeInfoConverter createTypeInfoConverter() {
    return new BitSailTypeInfoConverter();
  }

  @Override
  public Optional<WriterCommitter<String>> createCommitter() {
    return Optional.of(new PrintWriterCommitter());
  }
}
