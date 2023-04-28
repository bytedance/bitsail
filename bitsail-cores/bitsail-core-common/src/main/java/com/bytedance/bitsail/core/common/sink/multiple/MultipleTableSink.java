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

package com.bytedance.bitsail.core.common.sink.multiple;

import com.bytedance.bitsail.base.catalog.CatalogFactory;
import com.bytedance.bitsail.base.connector.BuilderGroup;
import com.bytedance.bitsail.base.connector.writer.v1.Sink;
import com.bytedance.bitsail.base.connector.writer.v1.Writer;
import com.bytedance.bitsail.base.connector.writer.v1.WriterCommitter;
import com.bytedance.bitsail.base.extension.SupportMultipleSinkTable;
import com.bytedance.bitsail.base.serializer.BinarySerializer;
import com.bytedance.bitsail.common.catalog.table.Catalog;
import com.bytedance.bitsail.common.catalog.table.CatalogTable;
import com.bytedance.bitsail.common.catalog.table.TableId;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.option.WriterOptions;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.common.type.TypeInfoConverter;
import com.bytedance.bitsail.common.typeinfo.TypeInfoValueConverter;
import com.bytedance.bitsail.component.format.debezium.DebeziumRowFilterNamesDeserializationSchema;
import com.bytedance.bitsail.core.common.serializer.multiple.MultipleTableCommitSerializer;
import com.bytedance.bitsail.core.common.serializer.multiple.MultipleTableStateSerializer;
import com.bytedance.bitsail.core.common.sink.multiple.comittable.MultipleTableCommit;
import com.bytedance.bitsail.core.common.sink.multiple.state.MultipleTableState;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

public class MultipleTableSink<InputT, CommitT extends Serializable, WriterStateT extends Serializable>
    implements Sink<Row, MultipleTableCommit<CommitT>, MultipleTableState<WriterStateT>> {

  private static final Logger LOG = LoggerFactory.getLogger(MultipleTableSink.class);

  private final Sink<InputT, CommitT, WriterStateT> realSink;
  private final CatalogFactory factory;

  private Catalog catalog;
  private Map<TableId, CatalogTable> catalogTables;
  private BitSailConfiguration commonConfiguration;
  private BitSailConfiguration writerConfiguration;
  private Pattern patternOfTable;

  public MultipleTableSink(Sink<InputT, CommitT, WriterStateT> realSink,
                           CatalogFactory factory) {
    this.realSink = realSink;
    this.factory = Preconditions.checkNotNull(factory,
        String.format("The sink %s must implement catalog when support multiple table write.",
            realSink.getWriterName()));
  }

  @Override
  public void configure(BitSailConfiguration commonConfiguration, BitSailConfiguration writerConfiguration) throws Exception {
    this.commonConfiguration = commonConfiguration;
    this.writerConfiguration = writerConfiguration;
    this.patternOfTable = Pattern.compile(writerConfiguration.get(WriterOptions.BaseWriterOptions.TABLE_PATTERN));
    this.catalog = factory.createTableCatalog(BuilderGroup.WRITER, writerConfiguration);
    this.catalog.open(realSink.createTypeInfoConverter());
    this.catalogTables = Maps.newHashMap();

    List<TableId> tableIds = catalog.listTables();
    for (TableId tableId : tableIds) {
      if (patternOfTable.matcher(tableId.toString()).find()) {
        LOG.info("Match table {} of the pattern {}.", tableId, patternOfTable.pattern());
        CatalogTable catalogTable = catalog.getCatalogTable(tableId);
        catalogTables.put(tableId, catalogTable);
      }
    }
    this.realSink.configure(commonConfiguration, writerConfiguration);
  }

  @Override
  public Writer<Row, MultipleTableCommit<CommitT>, MultipleTableState<WriterStateT>> createWriter(Writer.Context<MultipleTableState<WriterStateT>> context)
      throws IOException {
    return new MultipleTableWriter<>(
        writerConfiguration,
        context,
        new TypeInfoValueConverter(commonConfiguration),
        (SupportMultipleSinkTable<InputT, CommitT, WriterStateT>) realSink,
        catalogTables,
        patternOfTable,
        new DebeziumRowFilterNamesDeserializationSchema(writerConfiguration));
  }

  @Override
  public Optional<WriterCommitter<MultipleTableCommit<CommitT>>> createCommitter() {
    return Optional.of(
        new MultipleTableCommitter<>(
            writerConfiguration,
            (SupportMultipleSinkTable<InputT, CommitT, WriterStateT>) realSink
        )
    );
  }

  @Override
  public String getWriterName() {
    return realSink.getWriterName();
  }

  @Override
  public TypeInfoConverter createTypeInfoConverter() {
    return realSink.createTypeInfoConverter();
  }

  @Override
  public BinarySerializer<MultipleTableCommit<CommitT>> getCommittableSerializer() {
    return new MultipleTableCommitSerializer<>(realSink.getCommittableSerializer());
  }

  @Override
  public BinarySerializer<MultipleTableState<WriterStateT>> getWriteStateSerializer() {
    return new MultipleTableStateSerializer<>(realSink.getWriteStateSerializer());
  }
}
