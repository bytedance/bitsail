/*
 *       Copyright 2022-2023 Bytedance Ltd. and/or its affiliates.
 *
 *       Licensed under the Apache License, Version 2.0 (the "License");
 *       you may not use this file except in compliance with the License.
 *       You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *       Unless required by applicable law or agreed to in writing, software
 *       distributed under the License is distributed on an "AS IS" BASIS,
 *       WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *       See the License for the specific language governing permissions and
 *       limitations under the License.
 */

package com.bytedance.bitsail.core.common.sink;

import com.bytedance.bitsail.base.connector.writer.v1.Writer;
import com.bytedance.bitsail.base.connector.writer.v1.WriterCommitter;
import com.bytedance.bitsail.base.extension.SupportMultipleSinkTable;
import com.bytedance.bitsail.common.catalog.table.TableId;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.option.WriterOptions;
import com.bytedance.bitsail.connector.print.sink.PrintSink;
import com.bytedance.bitsail.connector.print.sink.PrintWriter;

import java.util.Optional;

public class MultiTablePrintSink extends PrintSink
    implements SupportMultipleSinkTable<com.bytedance.bitsail.common.row.Row, java.lang.String, java.lang.Integer> {
  @Override
  public Writer<com.bytedance.bitsail.common.row.Row, java.lang.String, java.lang.Integer> createWriter(Writer.Context<java.lang.Integer> context,
                                                                                                        BitSailConfiguration templateConfiguration) {
    return new PrintWriter(templateConfiguration, context);
  }

  @Override
  public Optional<WriterCommitter<String>> createCommitter(BitSailConfiguration templateConfiguration) {
    return Optional.empty();
  }

  @Override
  public BitSailConfiguration applyTableId(BitSailConfiguration template, TableId tableId) {
    return template.set(WriterOptions.BaseWriterOptions.TABLE_NAME, tableId.getTable());
  }
}
