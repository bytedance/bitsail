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

package com.bytedance.bitsail.test.e2e;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.option.CommonOptions;
import com.bytedance.bitsail.connector.fake.option.FakeReaderOptions;
import com.bytedance.bitsail.test.e2e.annotation.ReuseContainers;
import com.bytedance.bitsail.test.e2e.datasource.ElasticsearchDataSource;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.nio.file.Paths;

@ReuseContainers(reuse = {"executor", "sink"})
public class FakeToElasticsearchE2ETest extends AbstractE2ETest {
  private static  final int TOTAL_COUNT = 300;
  private static final int RATE = 100;

  @Test
  public void testFakeToEsBatch() throws Exception {
    BitSailConfiguration jobConf = BitSailConfiguration.from(
        new File(Paths.get(getClass().getClassLoader()
            .getResource("fake_to_elasticsearch.json")
            .toURI()).toString()));

    jobConf.set(FakeReaderOptions.TOTAL_COUNT, TOTAL_COUNT);
    jobConf.set(FakeReaderOptions.RATE, RATE);

    // Check if there are 300 docs in elasticsearch.
    submitJob(jobConf,
        "test_fake_to_elasticsearch_batch",
        dataSource -> Assert.assertEquals(
            TOTAL_COUNT,
            ((ElasticsearchDataSource) dataSource).getCount()
        ));
  }

  @Test
  public void testFakeToEsStreaming() throws Exception {
    BitSailConfiguration jobConf = BitSailConfiguration.from(
        new File(Paths.get(getClass().getClassLoader()
            .getResource("fake_to_elasticsearch.json")
            .toURI()).toString()));

    jobConf.set(FakeReaderOptions.TOTAL_COUNT, TOTAL_COUNT);
    jobConf.set(FakeReaderOptions.RATE, RATE);

    // enable checkpoint
    jobConf.set(CommonOptions.JOB_TYPE, "STREAMING");
    jobConf.set(CommonOptions.CheckPointOptions.CHECKPOINT_ENABLE, true);
    jobConf.set(CommonOptions.CheckPointOptions.CHECKPOINT_INTERVAL, 10000L);

    // Check if there are 300 docs in elasticsearch.
    submitJob(jobConf,
        "test_fake_to_elasticsearch_streaming",
        dataSource -> Assert.assertEquals(
            TOTAL_COUNT,
            ((ElasticsearchDataSource) dataSource).getCount()
        ));
  }
}
