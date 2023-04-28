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

import com.bytedance.bitsail.base.connector.writer.v1.Writer;
import com.bytedance.bitsail.base.extension.SupportMultipleSinkTable;
import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.catalog.table.CatalogTable;
import com.bytedance.bitsail.common.catalog.table.CatalogTableColumn;
import com.bytedance.bitsail.common.catalog.table.CatalogTableSchema;
import com.bytedance.bitsail.common.catalog.table.TableId;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.exception.CommonErrorCode;
import com.bytedance.bitsail.common.option.WriterOptions;
import com.bytedance.bitsail.common.row.MultipleTableRow;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.common.typeinfo.RowTypeInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfoValueConverter;
import com.bytedance.bitsail.component.format.debezium.DebeziumRowDeserializationSchema;
import com.bytedance.bitsail.core.common.sink.multiple.comittable.MultipleTableCommit;
import com.bytedance.bitsail.core.common.sink.multiple.state.MultipleTableState;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class MultipleTableWriter<InputT, CommT extends Serializable, WriterStateT extends Serializable>
    implements Writer<Row, MultipleTableCommit<CommT>, MultipleTableState<WriterStateT>> {

  private static final Logger LOG = LoggerFactory.getLogger(MultipleTableWriter.class);

  private final SupportMultipleSinkTable<InputT, CommT, WriterStateT> supplier;
  private final Context<MultipleTableState<WriterStateT>> context;
  private final BitSailConfiguration templateConfiguration;
  private final Map<TableId, CatalogTable> catalogTables;
  private final DebeziumRowDeserializationSchema deserializationSchema;
  private final TypeInfoValueConverter valueConverter;
  private final Pattern patternOfTable;
  private final String database;

  private transient Map<TableId, Writer<InputT, CommT, WriterStateT>> restoredMultiTableWriters;
  private transient Map<TableId, Writer<InputT, CommT, WriterStateT>> processedMultiTableWriters;
  private transient Map<TableId, RowTypeInfo> tableIdRowTypeInfos;

  public MultipleTableWriter(BitSailConfiguration templateConfiguration,
                             Context<MultipleTableState<WriterStateT>> context,
                             TypeInfoValueConverter typeInfoValueConverter,
                             SupportMultipleSinkTable<InputT, CommT, WriterStateT> supplier,
                             Map<TableId, CatalogTable> catalogTables,
                             Pattern patternOfTable,
                             DebeziumRowDeserializationSchema deserializationSchema) {
    this.templateConfiguration = templateConfiguration;
    this.context = context;
    this.supplier = supplier;
    this.catalogTables = catalogTables;
    this.deserializationSchema = deserializationSchema;
    this.valueConverter = typeInfoValueConverter;
    this.processedMultiTableWriters = Maps.newConcurrentMap();
    this.restoredMultiTableWriters = Maps.newConcurrentMap();
    this.tableIdRowTypeInfos = Maps.newConcurrentMap();
    this.patternOfTable = patternOfTable;
    this.database = templateConfiguration.get(WriterOptions.BaseWriterOptions.DB_NAME);

    restore();
  }

  private void restore() {
    if (context.isRestored()) {
      for (MultipleTableState<WriterStateT> tableState : context.getRestoreStates()) {
        TableId tableId = tableState.getTableId();
        CatalogTable catalogTable = catalogTables.get(tableId);
        if (Objects.isNull(catalogTable)) {
          LOG.warn("Subtask {} table {} already be deleted, skip restore it from state.",
              context.getIndexOfSubTaskId(), tableId);
          continue;
        }
        Context<WriterStateT> clone = cloneWriterContext(catalogTable, context);
        BitSailConfiguration configuration = supplier.applyTableId(templateConfiguration, tableId);
        Writer<InputT, CommT, WriterStateT> writer = supplier.createWriter(clone, configuration);
        restoredMultiTableWriters.put(tableId, writer);
      }
    }
  }

  @Override
  public void write(Row element) throws IOException {
    MultipleTableRow multipleTableRow = MultipleTableRow.of(element);
    TableId tableId = TableId.of(multipleTableRow.getTableId());
    tableId = TableId.of(database, tableId.getTable());

    if (!(patternOfTable.matcher(tableId.getTable()).find())) {
      LOG.debug("Table {} not match with pattern: {}.", tableId.getTable(), patternOfTable.pattern());
      return;
    }

    Writer<InputT, CommT, WriterStateT> realWriter;
    if (processedMultiTableWriters.containsKey(tableId)) {
      realWriter = processedMultiTableWriters.get(tableId);
    } else if (restoredMultiTableWriters.containsKey(tableId)) {
      realWriter = restoredMultiTableWriters.get(tableId);
    } else {
      BitSailConfiguration current = supplier
          .applyTableId(templateConfiguration.clone(), tableId);

      CatalogTable catalogTable = catalogTables.get(tableId);
      if (Objects.isNull(catalogTable)) {
        //Not support new table in runtime just now.
        LOG.warn("Subtask {} discovered new table: {} from input.", context.getIndexOfSubTaskId(),
            tableId);
        LOG.warn("Subtask {} not support create new table: {} in runtime right now, skip it.", context.getIndexOfSubTaskId(),
            tableId);
        return;
      }
      LOG.info("Subtask {} create real writer for the table: {}.", context.getIndexOfSubTaskId(), tableId);

      Context<WriterStateT> clone = cloneWriterContext(catalogTable, context);

      realWriter = supplier.createWriter(clone, current);
      processedMultiTableWriters.put(tableId, realWriter);
    }

    RowTypeInfo rowTypeInfo = tableIdRowTypeInfos.get(tableId);
    Row deserialize = deserializationSchema.deserialize(
        multipleTableRow.getValue(),
        rowTypeInfo.getFieldNames());

    for (int index = 0; index < rowTypeInfo.getTypeInfos().length; index++) {

      try {
        //convert field to real writer type info.
        deserialize.setField(index,
            valueConverter.convertObject(
                deserialize.getField(index),
                rowTypeInfo.getTypeInfos()[index])
        );
      } catch (Exception e) {
        LOG.error("Subtask {} failed to convert field name {}'s value {} to dest type info {}.",
            context.getIndexOfSubTaskId(),
            rowTypeInfo.getFieldNames()[index],
            deserialize.getField(index),
            rowTypeInfo.getTypeInfos()[index], e);
        //handled as dirty record.
        throw BitSailException.asBitSailException(CommonErrorCode.CONVERT_NOT_SUPPORT,
            String.format("Subtask %s failed to convert field name %s to dest type info %S.",
                context.getIndexOfSubTaskId(),
                rowTypeInfo.getFieldNames()[index],
                rowTypeInfo.getTypeInfos()[index]));
      }
    }
    realWriter.write((InputT) deserialize);
  }

  private Context<WriterStateT> cloneWriterContext(CatalogTable catalogTable,
                                                   Context<MultipleTableState<WriterStateT>> context) {
    return new Context<WriterStateT>() {

      @Override
      public RowTypeInfo getRowTypeInfo() {
        if (tableIdRowTypeInfos.containsKey(catalogTable.getTableId())) {
          return tableIdRowTypeInfos.get(catalogTable.getTableId());
        }
        CatalogTableSchema catalogTableSchema = catalogTable.getCatalogTableSchema();
        List<CatalogTableColumn> columns = catalogTableSchema.getColumns();
        String[] fieldNames = columns.stream()
            .map(CatalogTableColumn::getName)
            .collect(Collectors.toList())
            .toArray(new String[] {});

        TypeInfo<?>[] fieldTypes = columns.stream()
            .map(CatalogTableColumn::getType)
            .collect(Collectors.toList())
            .toArray(new TypeInfo<?>[] {});

        RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldNames, fieldTypes);
        tableIdRowTypeInfos.put(catalogTable.getTableId(), rowTypeInfo);
        return rowTypeInfo;
      }

      @Override
      public int getIndexOfSubTaskId() {
        return context.getIndexOfSubTaskId();
      }

      @Override
      public boolean isRestored() {
        for (MultipleTableState<WriterStateT> tableState : context.getRestoreStates()) {
          if (Objects.equals(tableState.getTableId(), catalogTable.getTableId())) {
            return true;
          }
        }
        return false;
      }

      @Override
      public List<WriterStateT> getRestoreStates() {
        for (MultipleTableState<WriterStateT> tableState : context.getRestoreStates()) {
          if (Objects.equals(tableState.getTableId(), catalogTable.getTableId())) {
            return tableState.getState();
          }
        }
        return Collections.emptyList();
      }
    };
  }

  @Override
  public void flush(boolean endOfInput) throws IOException {
    for (Writer<?, ?, ?> writer : processedMultiTableWriters.values()) {
      writer.flush(endOfInput);
    }
  }

  @Override
  public List<MultipleTableCommit<CommT>> prepareCommit() throws IOException {
    List<MultipleTableCommit<CommT>> prepared = Lists.newArrayList();
    for (TableId tableId : processedMultiTableWriters.keySet()) {
      List<CommT> commit = processedMultiTableWriters.get(tableId).prepareCommit();
      MultipleTableCommit<CommT> tableCommit = MultipleTableCommit
          .<CommT>builder()
          .commits(commit)
          .tableId(tableId)
          .build();
      prepared.add(tableCommit);
    }
    if (MapUtils.isNotEmpty(restoredMultiTableWriters)) {
      for (TableId tableId : restoredMultiTableWriters.keySet()) {
        if (processedMultiTableWriters.containsKey(tableId)) {
          continue;
        }
        Writer<InputT, CommT, WriterStateT> writer = restoredMultiTableWriters.get(tableId);
        MultipleTableCommit<CommT> tableCommit = MultipleTableCommit
            .<CommT>builder()
            .commits(writer.prepareCommit())
            .tableId(tableId)
            .build();
        prepared.add(tableCommit);
        //close it.
        writer.close();
      }
      //clear restored multi writer.
      restoredMultiTableWriters.clear();
    }
    return prepared;
  }

  @Override
  public List<MultipleTableState<WriterStateT>> snapshotState(long checkpointId) throws IOException {
    List<MultipleTableState<WriterStateT>> states = Lists.newArrayList();
    for (TableId tableId : processedMultiTableWriters.keySet()) {
      List<WriterStateT> state = processedMultiTableWriters.get(tableId).snapshotState(checkpointId);

      MultipleTableState<WriterStateT> tableState = MultipleTableState
          .<WriterStateT>builder()
          .tableId(tableId)
          .state(state)
          .build();

      states.add(tableState);
    }

    return states;
  }

  @Override
  public void close() throws IOException {
    for (Writer<?, ?, ?> writer : processedMultiTableWriters.values()) {
      writer.close();
    }
  }

  @VisibleForTesting
  public Map<TableId, Writer<InputT, CommT, WriterStateT>> getProcessedMultiTableWriters() {
    return processedMultiTableWriters;
  }

  @VisibleForTesting
  public Map<TableId, RowTypeInfo> getTableIdRowTypeInfos() {
    return tableIdRowTypeInfos;
  }
}
