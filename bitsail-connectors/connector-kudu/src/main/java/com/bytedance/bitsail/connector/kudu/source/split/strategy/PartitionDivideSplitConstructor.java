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
import com.bytedance.bitsail.connector.kudu.constants.KuduConstants;
import com.bytedance.bitsail.connector.kudu.option.KuduReaderOptions;
import com.bytedance.bitsail.connector.kudu.source.split.AbstractKuduSplitConstructor;
import com.bytedance.bitsail.connector.kudu.source.split.KuduSourceSplit;
import com.bytedance.bitsail.connector.kudu.util.KuduKeyEncoderUtils;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduPredicate;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.Partition;
import org.apache.kudu.client.PartitionSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

public class PartitionDivideSplitConstructor extends AbstractKuduSplitConstructor {
  private static final Logger LOG = LoggerFactory.getLogger(PartitionDivideSplitConstructor.class);

  private SplitConfiguration splitConf = null;
  private boolean available = false;
  private transient List<Partition> rangePartitions = null;

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
    if (splitConf.timeOutMills == null || splitConf.timeOutMills <= 0) {
      splitConf.setTimeOutMills(KuduConstants.DEFAULT_TIME_OUT_MILLS);
    }
    KuduTable kuduTable = client.openTable(this.tableName);
    this.rangePartitions = kuduTable.getRangePartitions(splitConf.getTimeOutMills());
    if (rangePartitions.size() <= 0) {
      LOG.warn("Partition cannot be empty.");
      return false;
    }
    if (this.rangePartitions.size() < splitConf.getSplitNum()) {
      this.splitConf.setSplitNum(this.rangePartitions.size());
      LOG.info("Resize split num to {}.", splitConf.getSplitNum());
    }
    return true;
  }

  @Override
  public List <KuduSourceSplit> construct(KuduClient kuduClient) throws Exception {
    List<KuduSourceSplit> splits = new ArrayList <>();

    KuduTable kuduTable = kuduClient.openTable(this.tableName);
    PartitionSchema partitionSchema = kuduTable.getPartitionSchema();
    PartitionSchema.RangeSchema rangeSchema = partitionSchema.getRangeSchema();

    for (int i = 0; i < this.rangePartitions.size(); i++) {
      KuduSourceSplit split = new KuduSourceSplit(i);
      PartialRow lowerPartialRow = KuduKeyEncoderUtils.decodeRangePartitionKey(schema, partitionSchema,
          ByteBuffer.wrap(rangePartitions.get(i).getPartitionKeyStart()).order(ByteOrder.BIG_ENDIAN));
      PartialRow upperPartialRow = KuduKeyEncoderUtils.decodeRangePartitionKey(schema, partitionSchema,
          ByteBuffer.wrap(rangePartitions.get(i).getPartitionKeyEnd()).order(ByteOrder.BIG_ENDIAN));

      for (int columnId : rangeSchema.getColumnIds()) {
        split.addPredicate(KuduPredicate.newComparisonPredicate(
            schema.getColumnByIndex(schema.getColumnIndex(columnId)),
            KuduPredicate.ComparisonOp.GREATER_EQUAL,
            getColumnValueInPartialRow(schema, columnId, lowerPartialRow)));
        split.addPredicate(KuduPredicate.newComparisonPredicate(
            schema.getColumnByIndex(schema.getColumnIndex(columnId)),
            KuduPredicate.ComparisonOp.LESS,
            getColumnValueInPartialRow(schema, columnId, upperPartialRow)));
      }
      splits.add(split);
      LOG.info(">>> the {}-th split is: {}", i, splits.get(i).toFormatString(schema));
    }
    LOG.info("Finally get {} splits.", splits.size());
    return splits;
  }

  private Object getColumnValueInPartialRow(Schema schema, int columnId, PartialRow partialRow) {
    Object val;
    ColumnSchema columnSchema = schema.getColumnByIndex(schema.getColumnIndex(columnId));
    switch (columnSchema.getType()) {
      case INT8:
      case INT16:
      case INT32:
        val = partialRow.getInt(columnSchema.getName());
        break;
      case DATE: {
        val = partialRow.getDate(columnSchema.getName());
        break;
      }
      case INT64:
      case UNIXTIME_MICROS:
        val = partialRow.getLong(columnSchema.getName());
        break;
      case BINARY: {
        val = partialRow.getBinary(columnSchema.getName());
        break;
      }
      case VARCHAR: {
        val = partialRow.getVarchar(columnSchema.getName());
        break;
      }
      case STRING: {
        val = partialRow.getString(columnSchema.getName());
        break;
      }
      case DECIMAL: {
        val = partialRow.getDecimal(columnSchema.getName());
        break;
      }
      default:
        throw new IllegalArgumentException(String.format(
            "The column type %s is not a valid key component type",
            columnSchema.getType()));
    }
    return val;
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
  @ToString(of = {"splitNum", "timeOutMills"})
  public static class SplitConfiguration {
    @JsonProperty("split_num")
    private Integer splitNum;

    @JsonProperty("time_out_mills")
    private Long timeOutMills;

    public boolean isValid(KuduTable table) {
      PartitionSchema partitionSchema = table.getPartitionSchema();

      if (partitionSchema == null) {
        LOG.warn("Partition schema cannot be empty.");
        return false;
      }
      if (partitionSchema.getRangeSchema() == null || CollectionUtils.isEmpty(partitionSchema.getRangeSchema().getColumnIds())) {
        LOG.warn("Partition rangeSchema cannot be empty.");
        return false;
      }
      return true;
    }
  }
}