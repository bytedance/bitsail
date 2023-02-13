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

package com.bytedance.bitsail.connector.clickhouse.source;

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
import com.bytedance.bitsail.connector.clickhouse.constant.ClickhouseConstants;
import com.bytedance.bitsail.connector.clickhouse.option.ClickhouseReaderOptions;
import com.bytedance.bitsail.connector.clickhouse.source.reader.ClickhouseSourceReader;
import com.bytedance.bitsail.connector.clickhouse.source.split.ClickhouseSourceSplit;
import com.bytedance.bitsail.connector.clickhouse.source.split.coordinator.ClickhouseSourceSplitCoordinator;
import com.bytedance.bitsail.connector.clickhouse.source.split.strategy.SimpleDivideSplitConstructor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ClickhouseSource implements Source<Row, ClickhouseSourceSplit, EmptyState>, ParallelismComputable {
  private static final Logger LOG = LoggerFactory.getLogger(ClickhouseSource.class);

  private BitSailConfiguration jobConf;

  @Override
  public void configure(ExecutionEnviron execution, BitSailConfiguration readerConfiguration) {
    this.jobConf = readerConfiguration;
  }

  @Override
  public Boundedness getSourceBoundedness() {
    return Boundedness.BOUNDEDNESS;
  }

  @Override
  public SourceReader<Row, ClickhouseSourceSplit> createReader(SourceReader.Context readerContext) {
    return new ClickhouseSourceReader(jobConf, readerContext);
  }

  @Override
  public SourceSplitCoordinator<ClickhouseSourceSplit, EmptyState> createSplitCoordinator(
      SourceSplitCoordinator.Context<ClickhouseSourceSplit, EmptyState> coordinatorContext) {
    return new ClickhouseSourceSplitCoordinator(coordinatorContext, jobConf);
  }

  @Override
  public String getReaderName() {
    return ClickhouseConstants.CLICKHOUSE_CONNECTOR_NAME;
  }

  @Override
  public TypeInfoConverter createTypeInfoConverter() {
    return new FileMappingTypeInfoConverter(getReaderName());
  }

  @Override
  public ParallelismAdvice getParallelismAdvice(BitSailConfiguration commonConf, BitSailConfiguration selfConf, ParallelismAdvice upstreamAdvice) {
    int parallelism;
    if (selfConf.fieldExists(ClickhouseReaderOptions.READER_PARALLELISM_NUM)) {
      parallelism = selfConf.get(ClickhouseReaderOptions.READER_PARALLELISM_NUM);
    } else {
      try {
        SimpleDivideSplitConstructor constructor = new SimpleDivideSplitConstructor(jobConf);
        parallelism = constructor.estimateSplitNum();
      } catch (IOException e) {
        parallelism = 1;
        LOG.warn("Failed to compute splits for computing parallelism, will use default 1.");
      }
    }
    return ParallelismAdvice.builder()
        .adviceParallelism(parallelism)
        .enforceDownStreamChain(false)
        .build();
  }
}
