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

package com.bytedance.bitsail.connector.kafka.source;

import com.bytedance.bitsail.base.connector.reader.v1.Boundedness;
import com.bytedance.bitsail.base.connector.reader.v1.Source;
import com.bytedance.bitsail.base.connector.reader.v1.SourceReader;
import com.bytedance.bitsail.base.connector.reader.v1.SourceSplitCoordinator;
import com.bytedance.bitsail.base.execution.ExecutionEnviron;
import com.bytedance.bitsail.base.execution.Mode;
import com.bytedance.bitsail.base.extension.ParallelismComputable;
import com.bytedance.bitsail.base.parallelism.ParallelismAdvice;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.option.CommonOptions;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.connector.kafka.source.coordinator.KafkaSourceSplitCoordinator;
import com.bytedance.bitsail.connector.kafka.source.reader.KafkaSourceReader;
import com.bytedance.bitsail.connector.kafka.source.split.KafkaSplit;
import com.bytedance.bitsail.connector.kafka.source.split.KafkaState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.bytedance.bitsail.connector.kafka.constants.KafkaConstants.CONNECTOR_TYPE_VALUE_KAFKA;

public class KafkaSource implements Source<Row, KafkaSplit, KafkaState>, ParallelismComputable {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaSource.class);

  private BitSailConfiguration readerConfiguration;

  private BitSailConfiguration commonConfiguration;

  @Override
  public void configure(ExecutionEnviron execution, BitSailConfiguration readerConfiguration) throws IOException {
    this.readerConfiguration = readerConfiguration;
    this.commonConfiguration = execution.getCommonConfiguration();
  }

  @Override
  public Boundedness getSourceBoundedness() {
    return Mode.BATCH.equals(Mode.getJobRunMode(commonConfiguration.get(CommonOptions.JOB_TYPE))) ?
        Boundedness.BOUNDEDNESS :
        Boundedness.UNBOUNDEDNESS;
  }

  @Override
  public SourceReader<Row, KafkaSplit> createReader(SourceReader.Context readerContext) {
    return new KafkaSourceReader(
        this.readerConfiguration,
        readerContext,
        getSourceBoundedness()
    );
  }

  @Override
  public SourceSplitCoordinator<KafkaSplit, KafkaState> createSplitCoordinator(SourceSplitCoordinator.Context<KafkaSplit, KafkaState> coordinatorContext) {
    return new KafkaSourceSplitCoordinator(
        coordinatorContext,
        this.readerConfiguration,
        getSourceBoundedness()
    );
  }

  @Override
  public String getReaderName() {
    return CONNECTOR_TYPE_VALUE_KAFKA;
  }

  @Override
  public ParallelismAdvice getParallelismAdvice(BitSailConfiguration commonConf, BitSailConfiguration selfConf, ParallelismAdvice upstreamAdvice) throws Exception {

    return ParallelismAdvice.builder()
        .adviceParallelism(1)
        .enforceDownStreamChain(true)
        .build();
  }
}
