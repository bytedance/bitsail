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

package com.bytedance.bitsail.connector.hadoop.source;

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
import com.bytedance.bitsail.common.type.BitSailTypeInfoConverter;
import com.bytedance.bitsail.common.type.TypeInfoConverter;
import com.bytedance.bitsail.connector.hadoop.constant.HadoopConstants;
import com.bytedance.bitsail.connector.hadoop.error.HadoopErrorCode;
import com.bytedance.bitsail.connector.hadoop.option.HadoopReaderOptions;
import com.bytedance.bitsail.connector.hadoop.source.coordinator.HadoopSourceSplitCoordinator;
import com.bytedance.bitsail.connector.hadoop.source.reader.HadoopSourceReader;
import com.bytedance.bitsail.connector.hadoop.source.split.HadoopSourceSplit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class HadoopSource implements Source<Row, HadoopSourceSplit, EmptyState>, ParallelismComputable {
  private static final Logger LOG = LoggerFactory.getLogger(HadoopSource.class);

  private BitSailConfiguration readerConfiguration;
  private List<String> hadoopPathList;

  @Override
  public void configure(ExecutionEnviron execution, BitSailConfiguration readerConfiguration) throws IOException {
    this.readerConfiguration = readerConfiguration;
    hadoopPathList = Arrays.asList(readerConfiguration.getNecessaryOption(HadoopReaderOptions.PATH_LIST, HadoopErrorCode.REQUIRED_VALUE).split(","));
  }

  @Override
  public Boundedness getSourceBoundedness() {
    return Boundedness.BOUNDEDNESS;
  }

  @Override
  public SourceReader<Row, HadoopSourceSplit> createReader(SourceReader.Context readerContext) {
    return new HadoopSourceReader(readerConfiguration, readerContext);
  }

  @Override
  public SourceSplitCoordinator<HadoopSourceSplit, EmptyState> createSplitCoordinator(SourceSplitCoordinator.Context<HadoopSourceSplit, EmptyState> coordinatorContext) {
    return new HadoopSourceSplitCoordinator(readerConfiguration, coordinatorContext, hadoopPathList);
  }

  @Override
  public String getReaderName() {
    return HadoopConstants.HADOOP_CONNECTOR_NAME;
  }

  @Override
  public TypeInfoConverter createTypeInfoConverter() {
    return new BitSailTypeInfoConverter();
  }

  @Override
  public ParallelismAdvice getParallelismAdvice(BitSailConfiguration commonConf, BitSailConfiguration selfConf, ParallelismAdvice upstreamAdvice) throws Exception {
    int adviceParallelism;
    if (selfConf.fieldExists(HadoopReaderOptions.READER_PARALLELISM_NUM)) {
      adviceParallelism = selfConf.get(HadoopReaderOptions.READER_PARALLELISM_NUM);
    } else {
      int parallelismThreshold = selfConf.get(HadoopReaderOptions.DEFAULT_HADOOP_PARALLELISM_THRESHOLD);
      adviceParallelism = Math.max(hadoopPathList.size() / parallelismThreshold, 1);
    }
    LOG.info("Parallelism for this job will set to {}.", adviceParallelism);
    return ParallelismAdvice.builder()
        .adviceParallelism(adviceParallelism)
        .enforceDownStreamChain(true)
        .build();
  }
}
