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

package com.bytedance.bitsail.flink.core.execution.configurer;

import com.bytedance.bitsail.base.catalog.TableCatalogFactory;
import com.bytedance.bitsail.base.catalog.TableCatalogFactoryHelper;
import com.bytedance.bitsail.base.connector.BuilderGroup;
import com.bytedance.bitsail.base.connector.reader.DataReaderDAGBuilder;
import com.bytedance.bitsail.base.connector.transform.DataTransformDAGBuilder;
import com.bytedance.bitsail.base.connector.writer.DataWriterDAGBuilder;
import com.bytedance.bitsail.base.extension.TypeInfoConverterFactory;
import com.bytedance.bitsail.common.catalog.TableCatalogManager;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.type.TypeInfoConverter;
import com.bytedance.bitsail.common.util.Preconditions;
import com.bytedance.bitsail.flink.core.execution.FlinkExecutionEnviron;

import lombok.AllArgsConstructor;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;

@AllArgsConstructor
public class FlinkDAGBuilderInterceptor {
  private static final Logger LOG = LoggerFactory.getLogger(FlinkDAGBuilderInterceptor.class);

  private static final int READER_AND_WRITER_NUMBER = 1;

  private final FlinkExecutionEnviron executionEnviron;

  public void intercept(List<DataReaderDAGBuilder> readerBuilders,
                        List<DataTransformDAGBuilder> transformDAGBuilders,
                        List<DataWriterDAGBuilder> writerBuilders) throws Exception {
    alignTableCatalog(
        readerBuilders,
        writerBuilders
    );

    executionEnviron.refreshConfiguration();

    configureDAGBuilders(readerBuilders, transformDAGBuilders, writerBuilders);
  }

  private void alignTableCatalog(List<DataReaderDAGBuilder> readerBuilders,
                                 List<DataWriterDAGBuilder> writerBuilders) throws Exception {
    //1. only support 1 source & 1 sink for align schema catalog.
    if (CollectionUtils.size(readerBuilders) != READER_AND_WRITER_NUMBER ||
        CollectionUtils.size(writerBuilders) != READER_AND_WRITER_NUMBER) {
      LOG.warn("Ignore align engine catalog, only support 1 reader and writer now.");
      return;
    }
    DataReaderDAGBuilder dataReaderDAGBuilder = readerBuilders.get(0);
    DataWriterDAGBuilder dataWriterDAGBuilder = writerBuilders.get(0);

    TableCatalogFactory readerCatalogFactory = TableCatalogFactoryHelper
        .getTableCatalogFactory(dataReaderDAGBuilder.getReaderName());

    TableCatalogFactory writerCatalogFactory = TableCatalogFactoryHelper
        .getTableCatalogFactory(dataWriterDAGBuilder.getWriterName());

    if (Objects.isNull(readerCatalogFactory) || Objects.isNull(writerCatalogFactory)) {
      LOG.warn("Ignore align engine catalog, reader or writer not support table catalog factory.");
      return;
    }

    if (!(dataReaderDAGBuilder instanceof TypeInfoConverterFactory) ||
        !(dataWriterDAGBuilder instanceof TypeInfoConverterFactory)) {
      LOG.warn("Ignore align engine catalog, reader or writer not support type info converter.");
      return;
    }

    TypeInfoConverter readerTypeInfoConverter = ((TypeInfoConverterFactory) dataReaderDAGBuilder)
        .createTypeInfoConverter();
    TypeInfoConverter writerTypeInfoConverter = ((TypeInfoConverterFactory) dataWriterDAGBuilder)
        .createTypeInfoConverter();

    if (Objects.isNull(readerTypeInfoConverter) || Objects.isNull(writerTypeInfoConverter)) {
      LOG.warn("Ignore align engine catalog, reader or writer type info converter is null.");
      return;
    }

    BitSailConfiguration readerConfiguration = executionEnviron.getReaderConfigurations().get(0);
    BitSailConfiguration writerConfiguration = executionEnviron.getWriterConfigurations().get(0);

    TableCatalogManager catalogManager = TableCatalogManager.builder()
        .readerTableCatalog(readerCatalogFactory.createTableCatalog(BuilderGroup.READER, executionEnviron, readerConfiguration))
        .writerTableCatalog(writerCatalogFactory.createTableCatalog(BuilderGroup.WRITER, executionEnviron, writerConfiguration))
        .readerTypeInfoConverter(readerTypeInfoConverter)
        .writerTypeInfoConverter(writerTypeInfoConverter)
        .commonConfiguration(executionEnviron.getCommonConfiguration())
        .readerConfiguration(readerConfiguration)
        .writerConfiguration(writerConfiguration)
        .build();

    catalogManager.alignmentCatalogTable();
  }

  /**
   * configure each of reader/writer DAG builders
   */
  private <T> void configureDAGBuilders(List<DataReaderDAGBuilder> readerBuilders,
                                        List<DataTransformDAGBuilder> transformBuilders,
                                        List<DataWriterDAGBuilder> writerBuilders) throws Exception {
    List<BitSailConfiguration> readerConfigurations = executionEnviron.getReaderConfigurations();
    List<BitSailConfiguration> transformConfigurations = executionEnviron.getTransformConfigurations();
    List<BitSailConfiguration> writerConfigurations = executionEnviron.getWriterConfigurations();
    Preconditions.checkState(readerBuilders.size() == readerConfigurations.size());
    Preconditions.checkState(transformBuilders.size() == transformConfigurations.size());
    Preconditions.checkState(writerBuilders.size() == writerConfigurations.size());
    for (int i = 0; i < readerBuilders.size(); ++i) {
      readerBuilders.get(i).configure(executionEnviron, readerConfigurations.get(i));
    }
    for (int i = 0; i < transformBuilders.size(); ++i) {
      transformBuilders.get(i).configure(executionEnviron, transformConfigurations.get(i));
    }
    for (int i = 0; i < writerBuilders.size(); ++i) {
      writerBuilders.get(i).configure(executionEnviron, writerConfigurations.get(i));
    }
  }
}
