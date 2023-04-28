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
import com.bytedance.bitsail.common.catalog.table.CatalogTable;
import com.bytedance.bitsail.common.catalog.table.CatalogTableColumn;
import com.bytedance.bitsail.common.catalog.table.CatalogTableSchema;
import com.bytedance.bitsail.common.catalog.table.TableId;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.option.WriterOptions;
import com.bytedance.bitsail.common.row.MultipleTableRow;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.common.typeinfo.RowTypeInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfoValueConverter;
import com.bytedance.bitsail.common.typeinfo.TypeInfos;
import com.bytedance.bitsail.component.format.debezium.DebeziumRowDeserializationSchema;
import com.bytedance.bitsail.core.common.sink.MultiTablePrintSink;
import com.bytedance.bitsail.core.common.sink.multiple.state.MultipleTableState;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class MultipleTableWriterTest {

  private MultiTablePrintSink multiTablePrintSink;
  private BitSailConfiguration jobConf;
  private Writer.Context<MultipleTableState<Integer>> context;
  private Map<TableId, CatalogTable> catalogTables;
  private TypeInfoValueConverter typeInfoValueConverter;
  private MultipleTableWriter<Row, String, Integer> multipleTableWriter;

  private CatalogTable catalogTable1;
  private CatalogTable catalogTable2;

  @Before
  public void before() {
    this.multiTablePrintSink = new MultiTablePrintSink();
    this.jobConf = BitSailConfiguration.newDefault();
    this.jobConf.set(WriterOptions.BaseWriterOptions.DB_NAME, "default");
    this.catalogTables = mockCatalogTables();
    this.typeInfoValueConverter = new TypeInfoValueConverter(BitSailConfiguration.newDefault());
    this.context = new Writer.Context<MultipleTableState<Integer>>() {
      @Override
      public RowTypeInfo getRowTypeInfo() {
        return null;
      }

      @Override
      public int getIndexOfSubTaskId() {
        return 0;
      }

      @Override
      public boolean isRestored() {
        return false;
      }

      @Override
      public List<MultipleTableState<Integer>> getRestoreStates() {
        return null;
      }
    };
    multipleTableWriter = new MultipleTableWriter<>(
        jobConf,
        context,
        typeInfoValueConverter,
        (SupportMultipleSinkTable<Row, String, Integer>) multiTablePrintSink,
        catalogTables,
        Pattern.compile("\\.*"),
        new DebeziumRowDeserializationSchema(jobConf));
  }

  private Map<TableId, CatalogTable> mockCatalogTables() {
    ConcurrentMap<TableId, CatalogTable> catalogTables = Maps.newConcurrentMap();

    catalogTable1 = CatalogTable.builder()
        .tableId(TableId.of("default", "test1"))
        .catalogTableSchema(CatalogTableSchema.builder()
            .columns(Lists.newArrayList(
                CatalogTableColumn.builder()
                    .type(TypeInfos.LONG_TYPE_INFO)
                    .name("int_type")
                    .build()
            )).build()
        ).build();
    catalogTable2 = CatalogTable.builder()
        .tableId(TableId.of("default", "test2"))
        .catalogTableSchema(CatalogTableSchema.builder()
            .columns(Lists.newArrayList(
                CatalogTableColumn.builder()
                    .type(TypeInfos.BIG_DECIMAL_TYPE_INFO)
                    .name("double_type")
                    .build()
            )).build()
        ).build();

    catalogTables.put(catalogTable1.getTableId(), catalogTable1);
    catalogTables.put(catalogTable2.getTableId(), catalogTable2);

    return catalogTables;
  }

  @Test
  public void testMultiTableWriter() throws IOException, URISyntaxException {
    for (CatalogTable catalogTable : catalogTables.values()) {
      multipleTableWriter.write(mockMultiTableRow(catalogTable));
    }
    Map<TableId, Writer<Row, String, Integer>> processedMultiTableWriters = multipleTableWriter.getProcessedMultiTableWriters();
    Assert.assertEquals(processedMultiTableWriters.values().size(), 2);

    validateRowTypeInfo(catalogTable1, multipleTableWriter.getTableIdRowTypeInfos().get(catalogTable1.getTableId()));
    validateRowTypeInfo(catalogTable2, multipleTableWriter.getTableIdRowTypeInfos().get(catalogTable2.getTableId()));
  }

  public static void validateRowTypeInfo(CatalogTable catalogTable, RowTypeInfo rowTypeInfo) {
    Assert.assertNotNull(catalogTable);
    Assert.assertNotNull(rowTypeInfo);

    TypeInfo<?>[] fieldTypes = catalogTable.getCatalogTableSchema().getColumns()
        .stream()
        .map(CatalogTableColumn::getType)
        .collect(Collectors.toList())
        .toArray(new TypeInfo<?>[] {});

    String[] fieldNames = catalogTable.getCatalogTableSchema().getColumns()
        .stream()
        .map(CatalogTableColumn::getName)
        .collect(Collectors.toList())
        .toArray(new String[] {});

    Assert.assertArrayEquals(fieldTypes, rowTypeInfo.getTypeInfos());
    Assert.assertArrayEquals(fieldNames, rowTypeInfo.getFieldNames());
  }

  private static Row mockMultiTableRow(CatalogTable catalogTable) throws URISyntaxException, IOException {
    MultipleTableRow multipleTableRow = MultipleTableRow.of(
        catalogTable.getTableId().toString(),
        StringUtils.EMPTY,
        mockValueJson(),
        String.valueOf(Long.MAX_VALUE),
        StringUtils.EMPTY
    );
    return multipleTableRow.asRow();
  }

  private static String mockValueJson() throws URISyntaxException, IOException {
    return new String(Files.readAllBytes(Paths.get(MultipleTableWriterTest.class
        .getClassLoader()
        .getResource("file/debezium_table1.json")
        .toURI()
        .getPath())));
  }
}