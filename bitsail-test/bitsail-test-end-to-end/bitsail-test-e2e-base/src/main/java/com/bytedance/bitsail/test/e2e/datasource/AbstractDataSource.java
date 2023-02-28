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

package com.bytedance.bitsail.test.e2e.datasource;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.test.e2e.base.AbstractContainer;
import com.bytedance.bitsail.test.e2e.base.transfer.TransferableFile;
import com.bytedance.bitsail.test.e2e.executor.AbstractExecutor;

import lombok.Setter;

import java.util.Set;

public abstract class AbstractDataSource extends AbstractContainer {

  protected Set<TransferableFile> transferableFiles;

  @Setter
  protected Role role;

  /**
   * Check if this data source can be used for the test.
   */
  public abstract boolean accept(BitSailConfiguration jobConf, Role role);

  /**
   * Configure the data source, like docker image name or version.
   */
  public abstract void configure(BitSailConfiguration dataSourceConf);

  /**
   * Add data source setting into job conf.
   */
  public abstract void modifyJobConf(BitSailConfiguration jobConf);

  /**
   * Start the data source.
   */
  public abstract void start();

  /**
   * Produce some data if it is for source.
   */
  public void fillData(AbstractExecutor executor) {

  }

  /**
   * Reset data source before reuse.
   */
  public void reset() {

  }

  enum Role {
    SOURCE,
    SINK
  }
}
