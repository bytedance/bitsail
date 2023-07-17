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

package com.bytedance.bitsail.core.flink116.bridge.writer.bounded;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.api.transformations.LegacySinkTransformation;
import org.apache.flink.streaming.api.transformations.PhysicalTransformation;

public class BoundedDataStreamSink<IN> extends DataStreamSink<IN> {

  public BoundedDataStreamSink(PhysicalTransformation<IN> transformation) {
    super(transformation);
  }

  static <IN> BoundedDataStreamSink<IN> forSinkFunction(DataStream<IN> inputStream, StreamSink<IN> sinkOperator) {
    final StreamExecutionEnvironment executionEnvironment =
        inputStream.getExecutionEnvironment();
    PhysicalTransformation<IN> transformation =
        new LegacySinkTransformation<>(
            inputStream.getTransformation(),
            "Unnamed",
            sinkOperator,
            executionEnvironment.getParallelism());
    executionEnvironment.addOperator(transformation);
    return new BoundedDataStreamSink<>(transformation);
  }
}
