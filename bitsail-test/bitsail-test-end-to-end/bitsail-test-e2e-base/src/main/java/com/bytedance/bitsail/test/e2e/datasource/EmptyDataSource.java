/*
 * Copyright 2022 Bytedance Ltd. and/or its affiliates.
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

package com.bytedance.bitsail.test.e2e.datasource;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;

import org.testcontainers.containers.Network;

public class EmptyDataSource extends AbstractDataSource {

  @Override
  public String getContainerName() {
    return "data-source:empty";
  }

  @Override
  public void initNetwork(Network executorNetwork) {

  }

  @Override
  public boolean accept(BitSailConfiguration jobConf, String sourceType) {
    return "empty".equalsIgnoreCase(sourceType);
  }

  @Override
  public void configure(BitSailConfiguration dataSourceConf) {

  }

  @Override
  public void modifyJobConf(BitSailConfiguration jobConf) {

  }

  @Override
  public void start() {

  }

  @Override
  public void fillData() {

  }
}
