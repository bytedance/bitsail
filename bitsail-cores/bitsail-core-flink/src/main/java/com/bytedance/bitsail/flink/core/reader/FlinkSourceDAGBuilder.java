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

package com.bytedance.bitsail.flink.core.reader;

import com.bytedance.bitsail.base.connector.reader.DataReaderDAGBuilder;
import com.bytedance.bitsail.base.connector.reader.v1.Source;
import com.bytedance.bitsail.base.connector.reader.v1.SourceSplit;
import com.bytedance.bitsail.base.execution.ExecutionEnviron;
import com.bytedance.bitsail.base.extension.ParallelismComputable;
import com.bytedance.bitsail.base.parallelism.ParallelismAdvice;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.flink.core.delagate.reader.source.DelegateFlinkSource;
import com.bytedance.bitsail.flink.core.execution.FlinkExecutionEnviron;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.io.Serializable;

public class FlinkSourceDAGBuilder<T, SplitT extends SourceSplit, StateT extends Serializable>
    implements DataReaderDAGBuilder, ParallelismComputable {

  private Source<T, SplitT, StateT> source;

  private DelegateFlinkSource<T, SplitT, StateT> delegateFlinkSource;

  public FlinkSourceDAGBuilder(Source<T, SplitT, StateT> source) {
    this.source = source;
  }

  @Override
  public void configure(ExecutionEnviron execution, BitSailConfiguration readerConfiguration) throws Exception {
    this.source.configure(execution, readerConfiguration);
    this.delegateFlinkSource = new DelegateFlinkSource<>(source,
        execution.getCommonConfiguration(),
        readerConfiguration);
  }

  public DataStream<T> fromSource(FlinkExecutionEnviron executionEnviron,
                                  int readerParallelism) {
    return executionEnviron.getExecutionEnvironment()
        //todo watermark
        .fromSource(delegateFlinkSource, WatermarkStrategy.noWatermarks(), source.getReaderName())
        .setParallelism(readerParallelism)
        .uid(source.getReaderName());
  }

  @Override
  public String getReaderName() {
    return source.getReaderName();
  }

  @Override
  public ParallelismAdvice getParallelismAdvice(BitSailConfiguration commonConf,
                                                BitSailConfiguration readerConfiguration,
                                                ParallelismAdvice upstreamAdvice) throws Exception {
    if (source instanceof ParallelismComputable) {
      return ((ParallelismComputable) source)
          .getParallelismAdvice(commonConf, readerConfiguration, upstreamAdvice);
    }
    //todo
    return null;
  }
}
