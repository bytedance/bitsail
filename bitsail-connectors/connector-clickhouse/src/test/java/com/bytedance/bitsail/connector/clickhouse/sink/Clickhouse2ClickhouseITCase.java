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

package com.bytedance.bitsail.connector.clickhouse.sink;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.connector.clickhouse.ClickhouseContainerHolder;
import com.bytedance.bitsail.connector.clickhouse.option.ClickhouseReaderOptions;
import com.bytedance.bitsail.connector.clickhouse.option.ClickhouseWriterOptions;
import com.bytedance.bitsail.test.connector.test.EmbeddedFlinkCluster;
import com.bytedance.bitsail.test.connector.test.utils.JobConfUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class Clickhouse2ClickhouseITCase {
  private static final int TOTAL_COUNT = 100;

  private ClickhouseContainerHolder containerHolder;

  @Before
  public void initClickhouse() throws Exception {
    containerHolder = new ClickhouseContainerHolder();
    containerHolder.start();
    containerHolder.createExampleSourceTable();
    containerHolder.insertData(TOTAL_COUNT);

    containerHolder.createExampleSinkTable();

  }

  @Test
  public void testClickhouseToPrint() throws Exception {
    BitSailConfiguration jobConf = JobConfUtils.fromClasspath("clickhouse_to_clickhouse.json");

    jobConf.set(ClickhouseReaderOptions.JDBC_URL, containerHolder.getJdbcHostUrl());
    jobConf.set(ClickhouseReaderOptions.USER_NAME, containerHolder.getUsername());
    jobConf.set(ClickhouseReaderOptions.PASSWORD, containerHolder.getPassword());

    jobConf.set(ClickhouseWriterOptions.JDBC_URL, containerHolder.getJdbcHostUrl());
    jobConf.set(ClickhouseWriterOptions.USER_NAME, containerHolder.getUsername());
    jobConf.set(ClickhouseWriterOptions.PASSWORD, containerHolder.getPassword());

    EmbeddedFlinkCluster.submitJob(jobConf);
  }

  @After
  public void close() {
    if (containerHolder != null) {
      containerHolder.close();
    }
  }
}
