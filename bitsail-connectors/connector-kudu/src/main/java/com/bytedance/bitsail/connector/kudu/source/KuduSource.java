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

package com.bytedance.bitsail.connector.kudu.source;

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
import com.bytedance.bitsail.connector.kudu.core.KuduConstants;
import com.bytedance.bitsail.connector.kudu.option.KuduReaderOptions;
import com.bytedance.bitsail.connector.kudu.source.reader.KuduSourceReader;
import com.bytedance.bitsail.connector.kudu.source.split.KuduSourceSplit;
import com.bytedance.bitsail.connector.kudu.source.split.coordinator.KuduSourceSplitCoordinator;
import com.bytedance.bitsail.connector.kudu.source.split.strategy.SimpleDivideSplitConstructor;
import com.bytedance.bitsail.connector.kudu.type.KuduTypeConverter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KuduSource implements Source<Row, KuduSourceSplit, EmptyState>, ParallelismComputable {
  private static final Logger LOG = LoggerFactory.getLogger(KuduSource.class);

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
  public SourceReader<Row, KuduSourceSplit> createReader(SourceReader.Context readerContext) {
    return new KuduSourceReader(jobConf, readerContext);
  }

  @Override
  public SourceSplitCoordinator<KuduSourceSplit, EmptyState> createSplitCoordinator(SourceSplitCoordinator.Context<KuduSourceSplit, EmptyState> coordinatorContext) {
    return new KuduSourceSplitCoordinator(coordinatorContext, jobConf);
  }

  @Override
  public String getReaderName() {
    return KuduConstants.KUDU_CONNECTOR_NAME;
  }

  @Override
  public TypeInfoConverter createTypeInfoConverter() {
    return new KuduTypeConverter();
  }

  @Override
  public ParallelismAdvice getParallelismAdvice(BitSailConfiguration commonConf, BitSailConfiguration selfConf, ParallelismAdvice upstreamAdvice) {
    int parallelism;
    if (selfConf.fieldExists(KuduReaderOptions.READER_PARALLELISM_NUM)) {
      parallelism = selfConf.get(KuduReaderOptions.READER_PARALLELISM_NUM);
    } else {
      parallelism = SimpleDivideSplitConstructor.getInstance(jobConf).estimateSplitNum();
    }

    return ParallelismAdvice.builder()
        .adviceParallelism(parallelism)
        .enforceDownStreamChain(false)
        .build();
  }
}
