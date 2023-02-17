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

package com.bytedance.bitsail.test.e2e.executor.flink;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;

import com.google.common.collect.Lists;
import lombok.NoArgsConstructor;

@NoArgsConstructor
public class Flink11Executor extends AbstractFlinkExecutor {

  private static final String FLINK_11_CORE_MODULE = "bitsail-cores/bitsail-core-flink-bridge";
  private static final String FLINK_11_CLIENT_MODULE = "bitsail-clients/bitsail-client-entry-flink";

  @Override
  public void configure(BitSailConfiguration executorConf) {
    this.coreModules = Lists.newArrayList(FLINK_11_CORE_MODULE);
    this.clientModules = Lists.newArrayList(FLINK_11_CLIENT_MODULE);
    super.configure(executorConf);
  }

  @Override
  public String getContainerName() {
    return "flink-1.11.6";
  }

  @Override
  protected String getFlinkDockerImage() {
    return "blockliu/flink-1.11.6-hadoop-3.1.1";
  }
}
