/*
 * Copyright [2022] [ByteDance]
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

package com.bytedance.bitsail.flink.core.operator;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.StreamSink;

public class BoundedStreamSinkOperator<IN> extends StreamSink<IN> implements BoundedOneInput {
  public BoundedStreamSinkOperator(SinkFunction<IN> sinkFunction) {
    super(sinkFunction);
  }

  @Override
  public void endInput() throws Exception {
    if (userFunction instanceof BoundedOneInput) {
      ((BoundedOneInput) userFunction).endInput();
    }
  }
}
