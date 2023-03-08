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

package com.bytedance.bitsail.test.integration.rocketmq;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.option.ReaderOptions;
import com.bytedance.bitsail.common.typeinfo.TypeInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfoUtils;
import com.bytedance.bitsail.connector.rocketmq.source.RocketMQSource;
import com.bytedance.bitsail.test.integration.AbstractIntegrationTest;
import com.bytedance.bitsail.test.integration.rocketmq.container.RocketMQDataSource;
import com.bytedance.bitsail.test.integration.utils.JobConfUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

@Ignore("Ignore unbounded streaming task.")
public class RocketMQSourceTest extends AbstractIntegrationTest {

  private RocketMQDataSource rocketMQDataSource;

  @Before
  public void initDataSource() throws Exception {
    rocketMQDataSource = new RocketMQDataSource();
    rocketMQDataSource.start();
  }

  @Test
  public void testRocketMQSource() throws Exception {
    BitSailConfiguration jobConf = JobConfUtils.fromClasspath("bitsail_rocketmq_print.json");
    RocketMQSource rocketMQSource = new RocketMQSource();
    TypeInfo<?>[] typeInfos = TypeInfoUtils.getTypeInfos(rocketMQSource.createTypeInfoConverter(),
        jobConf.get(ReaderOptions.BaseReaderOptions.COLUMNS));

    rocketMQDataSource.startProduceMessages(typeInfos);
    submitJob(jobConf);
  }
  
  @After
  public void closeDataSource() {
    if (rocketMQDataSource != null) {
      rocketMQDataSource.close();
    }
  }
}
