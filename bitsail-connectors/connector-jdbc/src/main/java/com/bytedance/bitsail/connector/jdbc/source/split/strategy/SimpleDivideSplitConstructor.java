/*
 *     Licensed to the Apache Software Foundation (ASF) under one or more
 *     contributor license agreements.  See the NOTICE file distributed with
 *     this work for additional information regarding copyright ownership.
 *     The ASF licenses this file to You under the Apache License, Version 2.0
 *     (the "License"); you may not use this file except in compliance with
 *     the License.  You may obtain a copy of the License at
 *        http://www.apache.org/licenses/LICENSE-2.0
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 */

package com.bytedance.bitsail.connector.jdbc.source.split.strategy;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.connector.jdbc.option.JdbcReaderOptions;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

@Slf4j
public class SimpleDivideSplitConstructor {

  protected final BitSailConfiguration jobConf;
  protected final String tableName;

  private SplitConfiguration splitConf;

  public SimpleDivideSplitConstructor(BitSailConfiguration jobConf) throws IOException {
    this.jobConf = jobConf;
    this.tableName = jobConf.get(JdbcReaderOptions.TABLE_NAME);

  }


  /**
   * Used for determine parallelism num.
   */
  public int estimateSplitNum() {
    int estimatedSplitNum = 1;
    if (splitConf != null && splitConf.getSplitNum() != null && splitConf.getSplitNum() > 0) {
      estimatedSplitNum = splitConf.getSplitNum();
    }
    log.info("Estimated split num is: {}", estimatedSplitNum);
    return estimatedSplitNum;
  }

  @NoArgsConstructor
  @Data
  @ToString(of = {"name", "splitNum", "lower", "upper"})
  public static class SplitConfiguration {
    @JsonProperty("name")
    private String name;

    @JsonProperty("lower_bound")
    private Long lower;

    @JsonProperty("upper_bound")
    private Long upper;

    @JsonProperty("split_num")
    private Integer splitNum;

    public long computeStep() {
      return Double.valueOf(Math.ceil((upper - lower + 1) * 1.0 / splitNum)).longValue();
    }
  }
}
