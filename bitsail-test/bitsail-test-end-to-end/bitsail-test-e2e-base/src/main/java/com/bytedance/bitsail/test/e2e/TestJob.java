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

package com.bytedance.bitsail.test.e2e;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.test.e2e.base.datasource.AbstractDataSource;
import com.bytedance.bitsail.test.e2e.base.executor.AbstractExecutor;

import lombok.AllArgsConstructor;
import lombok.Builder;

@AllArgsConstructor
@Builder
public class TestJob implements AutoCloseable {

  protected AbstractDataSource source;
  protected AbstractDataSource sink;
  protected AbstractExecutor executor;

  protected void prepareSource(BitSailConfiguration sourceConf) {
    source.configure(sourceConf);
    source.start();
    source.fillData();
  }

  protected void prepareSink(BitSailConfiguration sinkConf) {
    sink.configure(sinkConf);
    sink.start();
  }

  public int run(BitSailConfiguration testConf) throws Exception {
    prepareSource(testConf);
    prepareSink(testConf);

    executor.configure(testConf);
    executor.init();

    int exitCode;
    try {
      exitCode = executor.run("test");
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
    sink.validate();

    return exitCode;
  }

  @Override
  public void close() throws Exception {
    executor.close();
    source.close();
    sink.close();
  }
}
