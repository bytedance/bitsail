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

import com.bytedance.bitsail.base.execution.Mode;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.flink.core.FlinkJobMode;
import com.bytedance.bitsail.flink.core.execution.FlinkExecutionEnviron;
import com.bytedance.bitsail.flink.core.option.FlinkCommonOptions;
import com.bytedance.bitsail.flink.core.parallelism.FlinkParallelismAdvisor;

import org.junit.Test;
import org.mockito.Mockito;

public class StreamExecutionEnvironmentConfigurerTest {

  @Test
  public void testStreamExecutionEnvironmentConfigurer() {
    FlinkExecutionEnviron environ = new FlinkExecutionEnviron();
    environ.configure(Mode.STREAMING, null, BitSailConfiguration.newDefault());

    FlinkParallelismAdvisor advisor = Mockito.mock(FlinkParallelismAdvisor.class);
    Mockito.when(advisor.getGlobalParallelism()).thenReturn(2);
    environ.setParallelismAdvisor(advisor);

    BitSailConfiguration commonConf = BitSailConfiguration.newDefault();
    commonConf.set(FlinkCommonOptions.FLINK_MAX_PARALLELISM, 4);
    StreamExecutionEnvironmentConfigurer configurer = new StreamExecutionEnvironmentConfigurer(
        FlinkJobMode.STREAMING,
        environ,
        commonConf
    );
    configurer.prepareExecutionEnvironment();
  }
}
