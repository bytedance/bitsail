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

package com.bytedance.bitsail.connector.clickhouse.source.split.strategy;

import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.model.ColumnInfo;
import com.bytedance.bitsail.common.util.Pair;
import com.bytedance.bitsail.common.util.Preconditions;
import com.bytedance.bitsail.connector.clickhouse.error.ClickhouseErrorCode;
import com.bytedance.bitsail.connector.clickhouse.option.ClickhouseReaderOptions;
import com.bytedance.bitsail.connector.clickhouse.source.split.ClickhouseSourceSplit;
import com.bytedance.bitsail.connector.clickhouse.util.ClickhouseConnectionHolder;
import com.bytedance.bitsail.connector.clickhouse.util.ClickhouseJdbcUtils;

import com.clickhouse.jdbc.ClickHouseConnection;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class SimpleDivideSplitConstructor {
  private static final Logger LOG = LoggerFactory.getLogger(SimpleDivideSplitConstructor.class);

  private static final Set<String> SUPPORTED_TYPE = ImmutableSet.of(
      "INT8", "UINT8",
      "INT16", "UINT16",
      "INT", "INT32", "UINT32",
      "LONG", "INT64"
  );

  protected final BitSailConfiguration jobConf;
  protected final String tableName;

  private SplitConfiguration splitConf;

  public SimpleDivideSplitConstructor(BitSailConfiguration jobConf) throws IOException {
    this.jobConf = jobConf;
    this.tableName = jobConf.get(ClickhouseReaderOptions.TABLE_NAME);

    // construct basic split conf (name)
    if (!jobConf.fieldExists(ClickhouseReaderOptions.SPLIT_CONFIGURATION)) {
      this.splitConf = new SplitConfiguration();
    } else {
      String splitConfStr = jobConf.get(ClickhouseReaderOptions.SPLIT_CONFIGURATION);
      this.splitConf = new ObjectMapper().readValue(splitConfStr, SplitConfiguration.class);
    }

    String splitField = jobConf.get(ClickhouseReaderOptions.SPLIT_FIELD);
    if (StringUtils.isEmpty(splitField)) {
      this.splitConf = null;
      LOG.info("Split field is empty, will directly read the whole table.");
      return;
    }
    splitConf.setName(splitField);
    ColumnInfo splitColumn = jobConf.get(ClickhouseReaderOptions.COLUMNS)
        .stream().filter(col -> splitField.equals(col.getName()))
        .findFirst()
        .orElseThrow(() -> BitSailException.asBitSailException(ClickhouseErrorCode.CONFIG_ERROR,
            "Split field does not show in columns."));
    Preconditions.checkState(SUPPORTED_TYPE.contains(splitColumn.getType().trim().toUpperCase()), "Unsupported split column: " + splitColumn);

    // set split num
    if (splitConf.getSplitNum() == null || splitConf.getSplitNum() <= 0) {
      splitConf.setSplitNum(jobConf.getUnNecessaryOption(ClickhouseReaderOptions.READER_PARALLELISM_NUM, 1));
    }

    // set lower/upper bound
    boolean needUpdateLower = splitConf.getLower() == null;
    boolean needUpdateUpper = splitConf.getUpper() == null;

    if (needUpdateLower || needUpdateUpper) {
      Pair<Long, Long> newBound;
      try (ClickhouseConnectionHolder connectionHolder = new ClickhouseConnectionHolder(jobConf)) {
        newBound = queryBound(needUpdateLower, needUpdateUpper, jobConf, connectionHolder);
      } catch (Exception e) {
        LOG.error("Failed to refresh bound, will directly read whole table.", e);
        this.splitConf = null;
        return;
      }

      if (newBound.getFirst() != null) {
        splitConf.setLower(newBound.getFirst());
      }
      if (newBound.getSecond() != null) {
        splitConf.setUpper(newBound.getSecond());
      }
      LOG.info("Get final range: [{}, {}]", splitConf.getLower(), splitConf.getUpper());

      Long maxSplitNum = splitConf.getUpper() - splitConf.getLower() + 1;
      if (splitConf.getSplitNum() > maxSplitNum) {
        splitConf.setSplitNum(maxSplitNum.intValue());
        LOG.info("Resize split num to {}.", splitConf.getSplitNum());
      }
    }

    if (splitConf.getLower() > splitConf.getUpper()) {
      LOG.error("Found invalid split conf [{}], will directly read the table.", splitConf);
      splitConf = null;
    }

    // finish initialization
    LOG.info("SimpleDivideSplitConstructor is initialized.");
  }

  private Pair<Long, Long> queryBound(boolean queryMin,
                                      boolean queryMax,
                                      BitSailConfiguration jobConf,
                                      ClickhouseConnectionHolder holder) throws SQLException {
    String dbName = jobConf.getNecessaryOption(ClickhouseReaderOptions.DB_NAME,
        ClickhouseErrorCode.REQUIRED_VALUE);
    String tableName = jobConf.getNecessaryOption(ClickhouseReaderOptions.TABLE_NAME,
        ClickhouseErrorCode.REQUIRED_VALUE);
    String filterSql = jobConf.get(ClickhouseReaderOptions.SQL_FILTER);
    String splitField = splitConf.getName();

    String baseSql;
    if (queryMin && queryMax) {
      baseSql = ClickhouseJdbcUtils.getMinAndMaxQuerySql(dbName, tableName, splitField);
    } else if (queryMin) {
      baseSql = ClickhouseJdbcUtils.getMinQuerySql(dbName, tableName, splitField);
    } else if (queryMax) {
      baseSql = ClickhouseJdbcUtils.getMaxQuerySql(dbName, tableName, splitField);
    } else {
      return Pair.newPair(null, null);
    }
    String finalSql = ClickhouseJdbcUtils.decorateSql(baseSql, splitField, filterSql, null, false);

    ClickHouseConnection connection = holder.connect();
    Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery(finalSql);

    resultSet.next();
    if (queryMin && queryMax) {
      return Pair.newPair(resultSet.getLong(1), resultSet.getLong(2));
    } else if (queryMin) {
      return Pair.newPair(resultSet.getLong(1), null);
    } else {
      return Pair.newPair(null, resultSet.getLong(1));
    }
  }

  public List<ClickhouseSourceSplit> construct() throws IOException {

    if (this.splitConf == null || splitConf.getLower() == null || splitConf.getUpper() == null) {
      ClickhouseSourceSplit split = new ClickhouseSourceSplit(0);
      split.setReadTable(true);
      LOG.info("Split configuration is invalid, will read the whole table.");
      return Collections.singletonList(split);
    }

    List<ClickhouseSourceSplit> splitList = new ArrayList<>();
    int index = 0;
    long step = splitConf.computeStep();
    long beg = splitConf.getLower();

    while (beg <= splitConf.getUpper()) {
      ClickhouseSourceSplit split = new ClickhouseSourceSplit(index++);
      split.setLower(beg);
      split.setUpper(beg + step - 1);
      splitList.add(split);
      beg = beg + step;
    }

    LOG.info("Finally get {} splits.", splitList.size());
    for (int i = 0; i < splitList.size(); ++i) {
      LOG.info(">>> the {}-th split is: {}", i, splitList.get(i));
    }
    return splitList;
  }

  /**
   * Used for determine parallelism num.
   */
  public int estimateSplitNum() {
    int estimatedSplitNum = 1;
    if (splitConf != null && splitConf.getSplitNum() != null && splitConf.getSplitNum() > 0) {
      estimatedSplitNum = splitConf.getSplitNum();
    }
    LOG.info("Estimated split num is: {}", estimatedSplitNum);
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
