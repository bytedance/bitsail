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

package com.bytedance.bitsail.connector.mongodb.source;

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
import com.bytedance.bitsail.connector.mongodb.constant.MongoDBConstants;
import com.bytedance.bitsail.connector.mongodb.converter.MongoDBTypeInfoConverter;
import com.bytedance.bitsail.connector.mongodb.source.reader.MongoDBSourceReader;
import com.bytedance.bitsail.connector.mongodb.source.split.MongoDBSourceSplit;
import com.bytedance.bitsail.connector.mongodb.source.split.MongoDBSourceSplitCoordinator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Source of MongoDB
 */
public class MongoDBSource implements Source<Row, MongoDBSourceSplit, EmptyState>, ParallelismComputable {

  private static final Logger LOG = LoggerFactory.getLogger(MongoDBSource.class);
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
  public SourceReader<Row, MongoDBSourceSplit> createReader(SourceReader.Context readerContext) {
    return new MongoDBSourceReader(jobConf, readerContext);
  }

  @Override
  public SourceSplitCoordinator<MongoDBSourceSplit, EmptyState> createSplitCoordinator(
      SourceSplitCoordinator.Context<MongoDBSourceSplit, EmptyState> coordinatorContext) {
    return new MongoDBSourceSplitCoordinator(coordinatorContext, jobConf);
  }

  @Override
  public String getReaderName() {
    return MongoDBConstants.CONNECTOR_NAME;
  }

  @Override
  public TypeInfoConverter createTypeInfoConverter() {
    return new MongoDBTypeInfoConverter();
  }

  @Override
  public ParallelismAdvice getParallelismAdvice(BitSailConfiguration commonConf, BitSailConfiguration selfConf,
                                                ParallelismAdvice upstreamAdvice) {
    return new ParallelismAdvice(false, MongoDBSourceReader.getParallelismNum(selfConf));
  }

}
