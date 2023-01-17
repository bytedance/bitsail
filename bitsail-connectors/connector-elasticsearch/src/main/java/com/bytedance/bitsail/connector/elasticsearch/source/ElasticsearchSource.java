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

package com.bytedance.bitsail.connector.elasticsearch.source;

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
import com.bytedance.bitsail.connector.elasticsearch.base.EsConstants;
import com.bytedance.bitsail.connector.elasticsearch.option.ElasticsearchReaderOptions;
import com.bytedance.bitsail.connector.elasticsearch.source.coordinator.ElasticsearchSourceSplitCoordinator;
import com.bytedance.bitsail.connector.elasticsearch.source.reader.ElasticsearchReader;
import com.bytedance.bitsail.connector.elasticsearch.source.split.ElasticsearchSourceSplit;
import com.bytedance.bitsail.connector.elasticsearch.source.split.ElasticsearchSplitByIndexStrategy;
import com.bytedance.bitsail.connector.elasticsearch.source.split.ElasticsearchSplitStrategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ElasticsearchSource implements Source<Row, ElasticsearchSourceSplit, EmptyState>, ParallelismComputable {

  private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchSource.class);

  private BitSailConfiguration jobConf;

  @Override
  public void configure(ExecutionEnviron execution, BitSailConfiguration readerConfiguration) throws IOException {
    this.jobConf = readerConfiguration;
  }

  @Override
  public Boundedness getSourceBoundedness() {
    return Boundedness.BOUNDEDNESS;
  }

  @Override
  public SourceReader<Row, ElasticsearchSourceSplit> createReader(SourceReader.Context readerContext) {
    return new ElasticsearchReader(jobConf, readerContext);
  }

  @Override
  public SourceSplitCoordinator<ElasticsearchSourceSplit, EmptyState> createSplitCoordinator(
      SourceSplitCoordinator.Context<ElasticsearchSourceSplit, EmptyState> coordinatorContext) {
    return new ElasticsearchSourceSplitCoordinator(coordinatorContext, jobConf);
  }

  @Override
  public String getReaderName() {
    return EsConstants.ES_CONNECTOR_NAME;
  }

  @Override
  public TypeInfoConverter createTypeInfoConverter() {
    return new FileMappingTypeInfoConverter(getReaderName());
  }

  @Override
  public ParallelismAdvice getParallelismAdvice(BitSailConfiguration commonConf, BitSailConfiguration selfConf, ParallelismAdvice upstreamAdvice) throws Exception {
    int parallelism;
    if (selfConf.fieldExists(ElasticsearchReaderOptions.READER_PARALLELISM_NUM)) {
      parallelism = selfConf.get(ElasticsearchReaderOptions.READER_PARALLELISM_NUM);
      LOG.info("Use user-defined reader parallelism: {}", parallelism);
    } else {
      ElasticsearchSplitStrategy splitStrategy = new ElasticsearchSplitByIndexStrategy();
      parallelism = splitStrategy.estimateSplitNum(jobConf);
      LOG.info("Use index number as parallelism: {}", parallelism);
    }
    return new ParallelismAdvice(false, parallelism);
  }
}
