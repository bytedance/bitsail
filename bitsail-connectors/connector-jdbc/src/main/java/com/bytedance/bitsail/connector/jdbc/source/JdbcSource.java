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

package com.bytedance.bitsail.connector.jdbc.source;

import com.bytedance.bitsail.base.connector.reader.v1.Boundedness;
import com.bytedance.bitsail.base.connector.reader.v1.Source;
import com.bytedance.bitsail.base.connector.reader.v1.SourceReader;
import com.bytedance.bitsail.base.connector.reader.v1.SourceReader.Context;
import com.bytedance.bitsail.base.connector.reader.v1.SourceSplitCoordinator;
import com.bytedance.bitsail.base.connector.writer.v1.state.EmptyState;
import com.bytedance.bitsail.base.execution.ExecutionEnviron;
import com.bytedance.bitsail.base.extension.ParallelismComputable;
import com.bytedance.bitsail.base.parallelism.ParallelismAdvice;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.connector.jdbc.JdbcConstants;
import com.bytedance.bitsail.connector.jdbc.option.JdbcReaderOptions;
import com.bytedance.bitsail.connector.jdbc.source.reader.JdbcSourceReader;
import com.bytedance.bitsail.connector.jdbc.source.split.JdbcSourceSplit;
import com.bytedance.bitsail.connector.jdbc.source.split.coordinator.JdbcSourceSplitCoordinator;
import com.bytedance.bitsail.connector.jdbc.source.split.strategy.SimpleDivideSplitConstructor;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

@Slf4j
public class JdbcSource implements Source<Row, JdbcSourceSplit, EmptyState>, ParallelismComputable {

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
  public SourceReader<Row, JdbcSourceSplit> createReader(Context readerContext) {
    return new JdbcSourceReader(jobConf, readerContext);
  }

  @Override
  public SourceSplitCoordinator<JdbcSourceSplit, EmptyState> createSplitCoordinator(
      SourceSplitCoordinator.Context<JdbcSourceSplit, EmptyState> coordinatorContext) {
    return new JdbcSourceSplitCoordinator(coordinatorContext, jobConf);
  }

  @Override
  public String getReaderName() {
    return JdbcConstants.JDBC_CONNECTOR_NAME;
  }

  @Override
  public ParallelismAdvice getParallelismAdvice(BitSailConfiguration commonConf, BitSailConfiguration selfConf, ParallelismAdvice upstreamAdvice) {
    int parallelism;
    if (selfConf.fieldExists(JdbcReaderOptions.READER_PARALLELISM_NUM)) {
      parallelism = selfConf.get(JdbcReaderOptions.READER_PARALLELISM_NUM);
    } else {
      try {
        SimpleDivideSplitConstructor constructor = new SimpleDivideSplitConstructor(jobConf);
        parallelism = constructor.estimateSplitNum();
      } catch (IOException e) {
        parallelism = 1;
        log.warn("Failed to compute splits for computing parallelism, will use default 1.");
      }
    }
    return new ParallelismAdvice(false, parallelism);
  }
}
