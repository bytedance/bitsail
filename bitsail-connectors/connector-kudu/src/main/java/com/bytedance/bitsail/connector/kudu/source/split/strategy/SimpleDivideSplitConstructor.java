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

package com.bytedance.bitsail.connector.kudu.source.split.strategy;

import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.exception.CommonErrorCode;
import com.bytedance.bitsail.connector.kudu.option.KuduReaderOptions;
import com.bytedance.bitsail.connector.kudu.source.reader.KuduScannerConstructor;
import com.bytedance.bitsail.connector.kudu.source.split.AbstractKuduSplitConstructor;
import com.bytedance.bitsail.connector.kudu.source.split.KuduSourceSplit;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduPredicate;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.RowResult;
import org.apache.kudu.client.RowResultIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

public class SimpleDivideSplitConstructor extends AbstractKuduSplitConstructor {
  private static final Logger LOG = LoggerFactory.getLogger(SimpleDivideSplitConstructor.class);

  private static final Set<Type> SUPPORTED_TYPE = ImmutableSet.of(
      Type.INT8, Type.INT16, Type.INT32, Type.INT64
  );

  private SplitConfiguration splitConf = null;
  private boolean available = false;
  private Function<RowResult, Long> valueGetter;

  public SimpleDivideSplitConstructor(BitSailConfiguration jobConf, KuduClient client) throws IOException {
    super(jobConf, client);

    if (!jobConf.fieldExists(KuduReaderOptions.SPLIT_CONFIGURATION)) {
      LOG.warn("{} cannot work due to lack of split configuration.", this.getClass().getSimpleName());
      return;
    }
    String splitConfStr = jobConf.get(KuduReaderOptions.SPLIT_CONFIGURATION);
    this.splitConf = new ObjectMapper().readValue(splitConfStr, SplitConfiguration.class);

    this.available = splitConf.isValid(schema);
    if (!available) {
      LOG.warn("{} cannot work because split configuration is invalid.", this.getClass().getSimpleName());
      return;
    }

    this.valueGetter = splitConf.initKeyExtractor(schema);
    this.available = fillSplitConf(jobConf, client);
    if (!available) {
      return;
    }

    LOG.info("Successfully created ,final split configuration is: {}", splitConf);
  }

  @Override
  public boolean isAvailable() {
    return available;
  }

  @Override
  protected boolean fillSplitConf(BitSailConfiguration jobConf, KuduClient client) throws IOException {
    if (splitConf.getSplitNum() == null || splitConf.getSplitNum() <= 0) {
      splitConf.setSplitNum(jobConf.getUnNecessaryOption(KuduReaderOptions.READER_PARALLELISM_NUM, 1));
    }
    if (splitConf.getLower() != null && splitConf.getUpper() != null && splitConf.getLower() <= splitConf.getUpper()) {
      return true;
    }

    // If left or right border is not defined, scan the whole table's column to get min and max value.
    boolean needUpdateLower = splitConf.getLower() == null;
    boolean needUpdateUpper = splitConf.getUpper() == null;
    try {
      KuduScannerConstructor scannerConstructor = new KuduScannerConstructor(jobConf);
      KuduScanner scanner = scannerConstructor.createScannerForWholeTable(client, tableName);

      while (scanner.hasMoreRows()) {
        RowResultIterator results = scanner.nextRows();
        while (results.hasNext()) {
          RowResult result = results.next();
          Long value = valueGetter.apply(result);

          if (value == null) {
            LOG.error("Found null row in column {}, {} cannot work.", splitConf.getName(), this.getClass().getName());
            return false;
          }

          if (needUpdateLower) {
            splitConf.updateLower(value);
          }
          if (needUpdateUpper) {
            splitConf.updateUpper(value);
          }
        }
      }
      LOG.info("Get final range: [{}, {}]", splitConf.getLower(), splitConf.getUpper());
    } catch (KuduException e) {
      throw new IOException("Failed to get range of column " + splitConf.getName() + " from table " + tableName, e);
    }

    Long maxSplitNum = splitConf.getUpper() - splitConf.getLower() + 1;
    if (splitConf.getSplitNum() > maxSplitNum) {
      splitConf.setSplitNum(maxSplitNum.intValue());
      LOG.info("Resize split num to {}.", splitConf.getSplitNum());
    }
    return true;
  }

  @Override
  public List<KuduSourceSplit> construct(KuduClient kuduClient) throws IOException {
    List<KuduSourceSplit> splits = new ArrayList<>(splitConf.getSplitNum());

    int index = 0;
    long step = splitConf.computeStep();
    long beg = splitConf.getLower();
    ColumnSchema column = schema.getColumn(splitConf.getName());

    while (beg <= splitConf.getUpper()) {
      KuduPredicate lowerPred = KuduPredicate.newComparisonPredicate(column, KuduPredicate.ComparisonOp.GREATER_EQUAL, beg);
      KuduPredicate upperPred = KuduPredicate.newComparisonPredicate(column, KuduPredicate.ComparisonOp.LESS, beg + step);

      KuduSourceSplit split = new KuduSourceSplit(index++);
      split.addPredicate(lowerPred);
      split.addPredicate(upperPred);
      splits.add(split);

      beg = beg + step;
    }

    LOG.info("Finally get {} splits.", splits.size());
    for (int i = 0; i < splits.size(); ++i) {
      LOG.info(">>> the {}-th split is: {}", i, splits.get(i).toFormatString(schema));
    }
    return splits;
  }

  @Override
  public int estimateSplitNum() {
    int estimatedSplitNum = 1;
    if (splitConf.getSplitNum() != null && splitConf.getSplitNum() > 0) {
      estimatedSplitNum = splitConf.getSplitNum();
    }
    LOG.info("Estimated split num is: {}", estimatedSplitNum);
    return estimatedSplitNum;
  }

  @NoArgsConstructor
  @AllArgsConstructor
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

    public void updateLower(long value) {
      if (lower == null || lower > value) {
        lower = value;
      }
    }

    public void updateUpper(long value) {
      if (upper == null || upper < value) {
        upper = value;
      }
    }

    public long computeStep() {
      return Double.valueOf(Math.ceil((upper - lower + 1) * 1.0 / splitNum)).longValue();
    }

    public boolean isValid(Schema schema) {
      if (StringUtils.isEmpty(name)) {
        LOG.warn("Split column name cannot be empty.");
        return false;
      }

      ColumnSchema columnSchema = schema.getColumn(name);
      if (columnSchema == null) {
        LOG.warn("Split column {} cannot be found in schema.", name);
        return false;
      }
      if (!columnSchema.isKey()) {
        LOG.warn("Split column {} is not primary key column.", name);
        return false;
      }

      Type type = columnSchema.getType();
      if (!SUPPORTED_TYPE.contains(type)) {
        LOG.warn("Split column type should be integer, but it's {} here.", type);
        return false;
      }

      return true;
    }

    public Function<RowResult, Long> initKeyExtractor(Schema schema) {
      Type type = schema.getColumn(name).getType();
      switch (type) {
        case INT8:
          return (RowResult row) -> (long) row.getByte(name);
        case INT16:
          return (RowResult row) -> (long) row.getShort(name);
        case INT32:
          return (RowResult row) -> (long) row.getInt(name);
        case INT64:
          return (RowResult row) -> row.getLong(name);
        default:
          throw new BitSailException(CommonErrorCode.INTERNAL_ERROR, "Type " + type + " is not supported");
      }
    }
  }
}
