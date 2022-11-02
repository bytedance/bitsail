/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.bytedance.bitsail.connector.rocketmq.source;

import com.bytedance.bitsail.base.connector.reader.v1.Boundedness;
import com.bytedance.bitsail.base.connector.reader.v1.Source;
import com.bytedance.bitsail.base.connector.reader.v1.SourceReader;
import com.bytedance.bitsail.base.connector.reader.v1.SourceSplitCoordinator;
import com.bytedance.bitsail.base.execution.ExecutionEnviron;
import com.bytedance.bitsail.base.extension.ParallelismComputable;
import com.bytedance.bitsail.base.parallelism.ParallelismAdvice;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.connector.rocketmq.source.coordinator.RocketMQSourceSplitCoordinator;
import com.bytedance.bitsail.connector.rocketmq.source.reader.RocketMQSourceReader;
import com.bytedance.bitsail.connector.rocketmq.source.split.RocketMQSplit;
import com.bytedance.bitsail.connector.rocketmq.source.split.RocketMQState;

public class RocketMQSource
    implements Source<Row, RocketMQSplit, RocketMQState>, ParallelismComputable {

  public BitSailConfiguration readerConfiguration;

  @Override
  public void configure(ExecutionEnviron execution, BitSailConfiguration readerConfiguration) {
    this.readerConfiguration = readerConfiguration;
  }

  @Override
  public Boundedness getSourceBoundedness() {
    //todo support more
    return Boundedness.UNBOUNDEDNESS;
  }

  @Override
  public SourceReader<Row, RocketMQSplit> createReader(SourceReader.Context readerContext) {
    return new RocketMQSourceReader(
        readerConfiguration,
        readerContext,
        getSourceBoundedness());
  }

  @Override
  public SourceSplitCoordinator<RocketMQSplit, RocketMQState> createSplitCoordinator(SourceSplitCoordinator
                                                                                         .Context<RocketMQSplit, RocketMQState> coordinatorContext) {
    return new RocketMQSourceSplitCoordinator(
        coordinatorContext,
        readerConfiguration,
        getSourceBoundedness());
  }

  @Override
  public String getReaderName() {
    return "rocketmq";
  }

  @Override
  public ParallelismAdvice getParallelismAdvice(BitSailConfiguration commonConf, BitSailConfiguration selfConf, ParallelismAdvice upstreamAdvice) throws Exception {
    return ParallelismAdvice.builder().adviceParallelism(1).build();
  }
}
