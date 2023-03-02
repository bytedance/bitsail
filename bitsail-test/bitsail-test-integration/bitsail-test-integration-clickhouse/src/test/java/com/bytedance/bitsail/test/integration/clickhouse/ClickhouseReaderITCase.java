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

package com.bytedance.bitsail.test.integration.clickhouse;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.connector.clickhouse.option.ClickhouseReaderOptions;
import com.bytedance.bitsail.connector.clickhouse.source.split.strategy.SimpleDivideSplitConstructor;
import com.bytedance.bitsail.test.integration.AbstractIntegrationTest;
import com.bytedance.bitsail.test.integration.clickhouse.container.ClickhouseContainerHolder;
import com.bytedance.bitsail.test.integration.utils.JobConfUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ClickhouseReaderITCase extends AbstractIntegrationTest {
  private static final int TOTAL_COUNT = 300;

  private ClickhouseContainerHolder containerHolder;

  @Before
  public void initClickhouse() throws Exception {
    containerHolder = new ClickhouseContainerHolder();
    containerHolder.start();
    containerHolder.createExampleTable();
    containerHolder.insertData(TOTAL_COUNT);
  }

  @Test
  public void testClickhouseToPrint() throws Exception {
    BitSailConfiguration jobConf = JobConfUtils.fromClasspath("clickhouse_to_print.json");

    jobConf.set(ClickhouseReaderOptions.JDBC_URL, containerHolder.getJdbcHostUrl());
    jobConf.set(ClickhouseReaderOptions.DB_NAME, containerHolder.getDatabase());
    jobConf.set(ClickhouseReaderOptions.TABLE_NAME, containerHolder.getTable());
    jobConf.set(ClickhouseReaderOptions.USER_NAME, containerHolder.getUsername());
    jobConf.set(ClickhouseReaderOptions.PASSWORD, containerHolder.getPassword());

    SimpleDivideSplitConstructor.SplitConfiguration splitConf = new SimpleDivideSplitConstructor.SplitConfiguration();
    splitConf.setName("id");
    splitConf.setSplitNum(3);
    splitConf.setLower((long) TOTAL_COUNT / 2);
    jobConf.set(ClickhouseReaderOptions.SPLIT_CONFIGURATION, new ObjectMapper().writeValueAsString(splitConf));

    submitJob(jobConf);
  }

  @After
  public void close() {
    if (containerHolder != null) {
      containerHolder.close();
    }
  }
}

