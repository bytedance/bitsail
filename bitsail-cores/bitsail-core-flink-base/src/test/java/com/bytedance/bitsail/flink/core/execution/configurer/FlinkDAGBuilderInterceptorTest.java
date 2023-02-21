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

package com.bytedance.bitsail.flink.core.execution.configurer;

import com.bytedance.bitsail.base.connector.reader.DataReaderDAGBuilder;
import com.bytedance.bitsail.base.connector.writer.DataWriterDAGBuilder;
import com.bytedance.bitsail.base.execution.Mode;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.flink.core.execution.FlinkExecutionEnviron;

import com.google.common.collect.Lists;
import org.junit.Test;

public class FlinkDAGBuilderInterceptorTest {

  @Test
  public void testFlinkDAGBuilderInteceptor() throws Exception {
    DataReaderDAGBuilder reader = () -> "fake-reader";
    DataWriterDAGBuilder writer = () -> "fake-writer";

    FlinkExecutionEnviron environ = new FlinkExecutionEnviron();
    environ.configure(Mode.BATCH, null, BitSailConfiguration.newDefault());

    FlinkDAGBuilderInterceptor interceptor = new FlinkDAGBuilderInterceptor(environ);
    interceptor.intercept(Lists.newArrayList(reader), Lists.newArrayList(), Lists.newArrayList(writer));
  }
}
