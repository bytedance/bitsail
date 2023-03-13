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

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.connector.kudu.option.KuduReaderOptions;
import com.bytedance.bitsail.connector.kudu.source.split.AbstractKuduSplitConstructor;
import com.bytedance.bitsail.connector.kudu.source.split.KuduSourceSplit;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduPredicate;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Partition;
import org.apache.kudu.client.PartitionSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class PartitionDivideSplitConstructor extends AbstractKuduSplitConstructor {
  private static final Logger LOG = LoggerFactory.getLogger(PartitionDivideSplitConstructor.class);

  private SplitConfiguration splitConf = null;
  private boolean available = false;
  private transient List<Partition> partitions = null;

  public PartitionDivideSplitConstructor(BitSailConfiguration jobConf, KuduClient client) throws Exception {
    super(jobConf, client);

    if (!jobConf.fieldExists(KuduReaderOptions.SPLIT_CONFIGURATION)) {
      LOG.warn("{} cannot work due to lack of split configuration.", this.getClass().getSimpleName());
      return;
    }
    String splitConfStr = jobConf.get(KuduReaderOptions.SPLIT_CONFIGURATION);
    this.splitConf = new ObjectMapper().readValue(splitConfStr, SplitConfiguration.class);
    KuduTable kuduTable = client.openTable(this.tableName);
    this.available = splitConf.isValid(kuduTable);
    if (!available) {
      LOG.warn("{} cannot work because split configuration is invalid.", this.getClass().getSimpleName());
      return;
    }
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
  @SuppressWarnings("checkstyle:MagicNumber")
  protected boolean fillSplitConf(BitSailConfiguration jobConf, KuduClient client) throws Exception {
    if (splitConf.getSplitNum() == null || splitConf.getSplitNum() <= 0) {
      splitConf.setSplitNum(jobConf.getUnNecessaryOption(KuduReaderOptions.READER_PARALLELISM_NUM, 1));
    }
    KuduTable kuduTable = client.openTable(this.tableName);
    this.partitions = kuduTable.getRangePartitions(splitConf.getTimeOut());
    if (this.partitions.size() < splitConf.getSplitNum()) {
      this.splitConf.setSplitNum(this.partitions.size());
      LOG.info("Resize split num to {}.", splitConf.getSplitNum());
    }
    if (splitConf.timeOut == null || splitConf.timeOut <= 0) {
      splitConf.setTimeOut(3000L);
    }
    return true;
  }

  @Override
  public List <KuduSourceSplit> construct(KuduClient kuduClient) throws Exception {
    List<KuduSourceSplit> splits = new ArrayList <>();
    ColumnSchema partitionColumn = schema.getColumn(splitConf.getPartitionKey());

    for (int i = 0; i < partitions.size(); i++) {
      KuduSourceSplit split = new KuduSourceSplit(i);
      split.addPredicate(KuduPredicate.newComparisonPredicate(
          partitionColumn,
          KuduPredicate.ComparisonOp.GREATER_EQUAL,
          partitions.get(i).getPartitionKeyStart()[splitConf.partitionKeyIndex]));
      split.addPredicate(KuduPredicate.newComparisonPredicate(
          partitionColumn,
          KuduPredicate.ComparisonOp.LESS,
          partitions.get(i).getPartitionKeyEnd()[splitConf.partitionKeyIndex]));
      splits.add(split);
      LOG.info(">>> the {}-th split is: {}", i, splits.get(i).toFormatString(schema));
    }
    LOG.info("Finally get {} splits.", partitions.size());

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
  @ToString(of = {"partitionKey", "type", "splitNum", "timeOut"})
  public static class SplitConfiguration {
    @JsonProperty("partitionKey")
    private String partitionKey;

    @JsonProperty("split_num")
    private Integer splitNum;

    @JsonProperty("time_out_mills")
    private Long timeOut;

    @JsonProperty("partitionKeyIndex")
    private int partitionKeyIndex;

    public boolean isValid(KuduTable table) {
      if (StringUtils.isEmpty(partitionKey)) {
        LOG.warn("Split partition key cannot be empty.");
        return false;
      }
      PartitionSchema partitionSchema = table.getPartitionSchema();
      Schema schema = table.getSchema();
      if (schema.getColumn(partitionKey) == null) {
        LOG.warn("Split column {} cannot be found in schema.", partitionKey);
        return false;
      }
      if (partitionSchema == null || partitionSchema.getRangeSchema() == null) {
        LOG.warn("Partition schema cannot be empty.");
        return false;
      }
      // check is simple range partition mode
      boolean isSimple = isSimpleMode(partitionSchema, schema);
      if (!isSimple) {
        LOG.warn("Partition divide split strategy just support simple range partition mode.");
        return false;
      }
      PartitionSchema.RangeSchema rangeSchema = partitionSchema.getRangeSchema();
      int idx = -1;
      for (int i = 0; i < rangeSchema.getColumnIds().size(); i++) {
        if (partitionKey.equals(schema.getColumnByIndex(i).getName())) {
          idx = i;
        }
      }
      if (idx == -1) {
        LOG.warn("Split column {} cannot be found in range schema.", partitionKey);
        return false;
      }
      partitionKeyIndex = idx;
      return true;
    }

    private boolean isSimpleMode(PartitionSchema partitionSchema, Schema schema) {
      boolean isSimple = partitionSchema.getHashBucketSchemas().isEmpty() &&
          partitionSchema.getRangeSchema().getColumnIds().size() == schema.getPrimaryKeyColumnCount();
      if (isSimple) {
        int i = 0;
        for (Integer id : partitionSchema.getRangeSchema().getColumnIds()) {
          if (schema.getColumnIndex(id) != i++) {
            isSimple = false;
            break;
          }
        }
      }
      return isSimple;
    }
  }
}