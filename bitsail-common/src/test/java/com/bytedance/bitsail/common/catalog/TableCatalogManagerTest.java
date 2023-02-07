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

package com.bytedance.bitsail.common.catalog;

import com.bytedance.bitsail.common.catalog.fake.FakeTableCatalog;
import com.bytedance.bitsail.common.catalog.table.CatalogTableColumn;
import com.bytedance.bitsail.common.catalog.table.CatalogTableDefinition;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.model.ColumnInfo;
import com.bytedance.bitsail.common.option.ReaderOptions;
import com.bytedance.bitsail.common.option.WriterOptions;
import com.bytedance.bitsail.common.type.BitSailTypeInfoConverter;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class TableCatalogManagerTest {

  private BitSailConfiguration commonConfiguration;
  private BitSailConfiguration readerConfiguration;
  private BitSailConfiguration writerConfiguration;

  @Before
  public void before() {
    commonConfiguration = BitSailConfiguration.newDefault();
    readerConfiguration = BitSailConfiguration.newDefault();
    writerConfiguration = BitSailConfiguration.newDefault();
  }

  @Test
  public void testTableCatalogAlignmentIntersect() throws Exception {
    List<ColumnInfo> readerColumns = Lists.newArrayList();
    List<ColumnInfo> writerColumns = Lists.newArrayList();

    readerColumns.add(ColumnInfo.builder()
        .name("name")
        .type("string")
        .build());

    readerColumns.add(ColumnInfo.builder()
        .name("id")
        .type("int")
        .build());

    writerColumns.add(ColumnInfo.builder()
        .name("name")
        .type("string")
        .build());

    commonConfiguration.set(TableCatalogOptions.COLUMN_ALIGN_STRATEGY,
        TableCatalogStrategy.INTERSECT.name());
    commonConfiguration.set(TableCatalogOptions.SYNC_DDL, true);
    FakeTableCatalog readerFakeTableCatalog = new FakeTableCatalog(readerColumns, CatalogTableDefinition.builder()
        .database("a")
        .table("b").build());
    FakeTableCatalog writerFakeTableCatalog = new FakeTableCatalog(writerColumns, CatalogTableDefinition.builder()
        .database("a")
        .table("c").build());

    TableCatalogManager tableCatalogManager = TableCatalogManager.builder()
        .readerTableCatalog(readerFakeTableCatalog)
        .writerTableCatalog(writerFakeTableCatalog)
        .writerTypeInfoConverter(new BitSailTypeInfoConverter())
        .readerTypeInfoConverter(new BitSailTypeInfoConverter())
        .commonConfiguration(commonConfiguration)
        .readerConfiguration(readerConfiguration)
        .writerConfiguration(writerConfiguration)
        .build();

    tableCatalogManager.alignmentCatalogTable();

    List<ColumnInfo> finalReaderColumns = readerConfiguration.get(ReaderOptions.BaseReaderOptions.COLUMNS);
    List<ColumnInfo> finalWriterColumns = writerConfiguration.get(WriterOptions.BaseWriterOptions.COLUMNS);

    Assert.assertEquals(finalReaderColumns.size(), finalWriterColumns.size());
  }

  @Test
  public void testTableCatalogAlignmentSourceOnly() throws Exception {
    List<ColumnInfo> readerColumns = Lists.newArrayList();
    List<ColumnInfo> writerColumns = Lists.newArrayList();

    readerColumns.add(ColumnInfo.builder()
        .name("name")
        .type("string")
        .build());

    readerColumns.add(ColumnInfo.builder()
        .name("id")
        .type("int")
        .build());

    writerColumns.add(ColumnInfo.builder()
        .name("name")
        .type("string")
        .build());

    commonConfiguration.set(TableCatalogOptions.COLUMN_ALIGN_STRATEGY,
        TableCatalogStrategy.SOURCE_ONLY.name());
    commonConfiguration.set(TableCatalogOptions.SYNC_DDL, true);

    FakeTableCatalog readerFakeTableCatalog = new FakeTableCatalog(readerColumns, CatalogTableDefinition.builder()
        .database("a")
        .table("b").build());
    FakeTableCatalog writerFakeTableCatalog = new FakeTableCatalog(writerColumns, CatalogTableDefinition.builder()
        .database("a")
        .table("c").build());

    TableCatalogManager tableCatalogManager = TableCatalogManager.builder()
        .readerTableCatalog(readerFakeTableCatalog)
        .writerTableCatalog(writerFakeTableCatalog)
        .writerTypeInfoConverter(new BitSailTypeInfoConverter())
        .readerTypeInfoConverter(new BitSailTypeInfoConverter())
        .commonConfiguration(commonConfiguration)
        .readerConfiguration(readerConfiguration)
        .writerConfiguration(writerConfiguration)
        .build();

    tableCatalogManager.alignmentCatalogTable();

    List<ColumnInfo> finalReaderColumns = readerConfiguration.get(ReaderOptions.BaseReaderOptions.COLUMNS);
    List<ColumnInfo> finalWriterColumns = writerConfiguration.get(WriterOptions.BaseWriterOptions.COLUMNS);

    Assert.assertEquals(finalReaderColumns.size(), finalWriterColumns.size());
    List<CatalogTableColumn> addedTableColumns = writerFakeTableCatalog.getAddedTableColumns();
    Assert.assertEquals(addedTableColumns.size(), 1);
  }

  @Test
  public void testColumnAlignmentUpdate() throws Exception {
    List<ColumnInfo> readerColumns = Lists.newArrayList();
    List<ColumnInfo> writerColumns = Lists.newArrayList();

    readerColumns.add(ColumnInfo.builder()
        .name("name")
        .type("int")
        .build());

    readerColumns.add(ColumnInfo.builder()
        .name("id")
        .type("int")
        .build());

    writerColumns.add(ColumnInfo.builder()
        .name("name")
        .type("string")
        .build());

    commonConfiguration.set(TableCatalogOptions.COLUMN_ALIGN_STRATEGY, TableCatalogStrategy.SOURCE_ONLY.name());
    commonConfiguration.set(TableCatalogOptions.SYNC_DDL, true);

    FakeTableCatalog readerFakeTableCatalog = new FakeTableCatalog(readerColumns, CatalogTableDefinition.builder()
        .database("a")
        .table("b").build());
    FakeTableCatalog writerFakeTableCatalog = new FakeTableCatalog(writerColumns, CatalogTableDefinition.builder()
        .database("a")
        .table("c").build());

    TableCatalogManager tableCatalogManager = TableCatalogManager.builder()
        .readerTableCatalog(readerFakeTableCatalog)
        .writerTableCatalog(writerFakeTableCatalog)
        .writerTypeInfoConverter(new BitSailTypeInfoConverter())
        .readerTypeInfoConverter(new BitSailTypeInfoConverter())
        .commonConfiguration(commonConfiguration)
        .readerConfiguration(readerConfiguration)
        .writerConfiguration(writerConfiguration)
        .build();

    tableCatalogManager.alignmentCatalogTable();

    List<ColumnInfo> finalReaderColumns = readerConfiguration.get(ReaderOptions.BaseReaderOptions.COLUMNS);
    List<ColumnInfo> finalWriterColumns = writerConfiguration.get(WriterOptions.BaseWriterOptions.COLUMNS);

    Assert.assertEquals(finalReaderColumns.size(), finalWriterColumns.size());
    List<CatalogTableColumn> addedTableColumns = writerFakeTableCatalog.getAddedTableColumns();
    Assert.assertEquals(addedTableColumns.size(), 1);
    Assert.assertEquals(writerFakeTableCatalog.getUpdatedTableColumns().size(), 1);
  }
}