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

package com.bytedance.bitsail.connector.doris.source;

import com.bytedance.bitsail.base.connector.reader.v1.Boundedness;
import com.bytedance.bitsail.base.connector.reader.v1.Source;
import com.bytedance.bitsail.base.connector.reader.v1.SourceReader;
import com.bytedance.bitsail.base.connector.reader.v1.SourceSplitCoordinator;
import com.bytedance.bitsail.base.connector.writer.v1.state.EmptyState;
import com.bytedance.bitsail.base.execution.ExecutionEnviron;
import com.bytedance.bitsail.base.extension.ParallelismComputable;
import com.bytedance.bitsail.base.parallelism.ParallelismAdvice;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.common.type.TypeInfoConverter;
import com.bytedance.bitsail.common.type.filemapping.FileMappingTypeInfoConverter;
import com.bytedance.bitsail.connector.doris.config.DorisExecutionOptions;
import com.bytedance.bitsail.connector.doris.config.DorisOptions;
import com.bytedance.bitsail.connector.doris.constant.DorisConstants;
import com.bytedance.bitsail.connector.doris.error.DorisErrorCode;
import com.bytedance.bitsail.connector.doris.option.DorisReaderOptions;
import com.bytedance.bitsail.connector.doris.source.reader.DorisSourceReader;
import com.bytedance.bitsail.connector.doris.source.split.DorisSourceSplit;
import com.bytedance.bitsail.connector.doris.source.split.coordinator.DorisSourceSplitCoordinator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DorisSource implements Source<Row, DorisSourceSplit, EmptyState>, ParallelismComputable {

  private static final Logger LOG = LoggerFactory.getLogger(DorisSource.class);
  private DorisOptions dorisOptions;
  private DorisExecutionOptions dorisExecutionOptions;
  private BitSailConfiguration jobConf;

  @Override
  public void configure(ExecutionEnviron execution, BitSailConfiguration readerConfiguration) {
    this.jobConf = readerConfiguration;
    initDorisOptions(readerConfiguration);
    initDorisExecutionOptions(readerConfiguration);
  }

  @Override
  public Boundedness getSourceBoundedness() {
    return Boundedness.BOUNDEDNESS;
  }

  @Override
  public SourceReader<Row, DorisSourceSplit> createReader(SourceReader.Context readerContext) {
    return new DorisSourceReader(readerContext, dorisExecutionOptions, dorisOptions);
  }

  @Override
  public SourceSplitCoordinator<DorisSourceSplit, EmptyState> createSplitCoordinator(
      SourceSplitCoordinator.Context<DorisSourceSplit, EmptyState> coordinatorContext) {
    return new DorisSourceSplitCoordinator(coordinatorContext, dorisExecutionOptions, dorisOptions);
  }

  @Override
  public String getReaderName() {
    return DorisConstants.DORIS_CONNECTOR_NAME;
  }

  @Override
  public TypeInfoConverter createTypeInfoConverter() {
    return new FileMappingTypeInfoConverter(getReaderName());
  }

  @Override
  public ParallelismAdvice getParallelismAdvice(BitSailConfiguration commonConf, BitSailConfiguration selfConf, ParallelismAdvice upstreamAdvice) {
    int parallelism = 1;
    if (selfConf.fieldExists(DorisReaderOptions.READER_PARALLELISM_NUM)) {
      parallelism = selfConf.get(DorisReaderOptions.READER_PARALLELISM_NUM);
    }
    return new ParallelismAdvice(false, parallelism);
  }

  private void initDorisOptions(BitSailConfiguration readerConfiguration) {
    LOG.info("Start to init DorisOptions!");
    DorisOptions.DorisOptionsBuilder builder = DorisOptions.builder()
        .feNodes(readerConfiguration.getNecessaryOption(DorisReaderOptions.FE_HOSTS, DorisErrorCode.READER_REQUIRED_VALUE))
        .mysqlNodes(readerConfiguration.get(DorisReaderOptions.MYSQL_HOSTS))
        .databaseName(readerConfiguration.getNecessaryOption(DorisReaderOptions.DB_NAME, DorisErrorCode.READER_REQUIRED_VALUE))
        .tableName(readerConfiguration.getNecessaryOption(DorisReaderOptions.TABLE_NAME, DorisErrorCode.READER_REQUIRED_VALUE))
        .username(readerConfiguration.getNecessaryOption(DorisReaderOptions.USER, DorisErrorCode.READER_REQUIRED_VALUE))
        .password(readerConfiguration.getNecessaryOption(DorisReaderOptions.PASSWORD, DorisErrorCode.READER_REQUIRED_VALUE))
        .columnInfos(readerConfiguration.get(DorisReaderOptions.COLUMNS));

    dorisOptions = builder.build();
  }

  private void initDorisExecutionOptions(BitSailConfiguration readerConfiguration) {
    LOG.info("Start to init DorisExecutionOptions!");
    final DorisExecutionOptions.DorisExecutionOptionsBuilder builder = DorisExecutionOptions.builder();
    builder.requestConnectTimeoutMs(readerConfiguration.get(DorisReaderOptions.REQUEST_CONNECT_TIMEOUTS))
        .requestRetries(readerConfiguration.get(DorisReaderOptions.REQUEST_RETRIES))
        .requestReadTimeoutMs(readerConfiguration.get(DorisReaderOptions.REQUEST_READ_TIMEOUTS))
        .sqlFilter(readerConfiguration.get(DorisReaderOptions.SQL_FILTER))
        .requestTabletSize(readerConfiguration.get(DorisReaderOptions.TABLET_SIZE))
        .execMemLimit(readerConfiguration.get(DorisReaderOptions.EXEC_MEM_LIMIT))
        .requestQueryTimeoutS(readerConfiguration.get(DorisReaderOptions.REQUEST_QUERY_TIMEOUT_S))
        .requestBatchSize(readerConfiguration.get(DorisReaderOptions.REQUEST_BATCH_SIZE));

    dorisExecutionOptions = builder.build();
  }
}
