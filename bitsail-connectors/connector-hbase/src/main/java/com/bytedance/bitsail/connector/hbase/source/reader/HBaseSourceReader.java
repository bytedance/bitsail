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

package com.bytedance.bitsail.connector.hbase.source.reader;

import com.bytedance.bitsail.base.connector.reader.v1.SourceEvent;
import com.bytedance.bitsail.base.connector.reader.v1.SourcePipeline;
import com.bytedance.bitsail.base.connector.reader.v1.SourceReader;
import com.bytedance.bitsail.base.format.DeserializationFormat;
import com.bytedance.bitsail.base.format.DeserializationSchema;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.constants.Constants;
import com.bytedance.bitsail.common.model.ColumnInfo;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.common.typeinfo.TypeInfo;
import com.bytedance.bitsail.common.util.Preconditions;
import com.bytedance.bitsail.connector.hbase.HBaseHelper;
import com.bytedance.bitsail.connector.hbase.error.HBasePluginErrorCode;
import com.bytedance.bitsail.connector.hbase.format.HBaseDeserializationFormat;
import com.bytedance.bitsail.connector.hbase.option.HBaseReaderOptions;
import com.bytedance.bitsail.connector.hbase.source.split.HBaseSourceSplit;

import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Deque;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class HBaseSourceReader implements SourceReader<Row, HBaseSourceSplit> {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseSourceReader.class);
  private static final String ROW_KEY = "rowkey";
  private final int subTaskId;

  /**
   * Used to de duplicate user-defined fields with the same name.
   */
  private transient Map<String, byte[][]> namesMap;

  /**
   * Schema Settings.
   */
  private String tableName;
  private final transient Connection connection;
  private ResultScanner currentScanner;
  private HBaseSourceSplit currentSplit;

  private boolean hasNoMoreSplits = false;
  private int totalSplitNum = 0;
  private final Deque<HBaseSourceSplit> splits;

  private TypeInfo<?>[] typeInfos;
  private List<String> columnNames;
  private Set<String> columnFamilies;
  private transient DeserializationFormat<byte[][], Row> deserializationFormat;
  private transient DeserializationSchema<byte[][], Row> deserializationSchema;

  /**
   * Parameters for Hbase/TableInputFormat.
   */
  private Map<String, Object> hbaseConfig;

  /**
   * Number of regions, used for computing parallelism.
   */
  private int regionCount;

  private static final Retryer<Object> RETRYER = RetryerBuilder.newBuilder()
      .retryIfException()
      .withWaitStrategy(WaitStrategies.fixedWait(Constants.RETRY_DELAY, TimeUnit.MILLISECONDS))
      .withStopStrategy(StopStrategies.stopAfterAttempt(Constants.RETRY_TIMES))
      .build();

  public HBaseSourceReader(BitSailConfiguration jobConf, SourceReader.Context readerContext, int subTaskId) {
    this.subTaskId = subTaskId;

    this.hbaseConfig = jobConf.get(HBaseReaderOptions.HBASE_CONF);
    this.tableName = jobConf.get(HBaseReaderOptions.TABLE);
    this.columnFamilies = new LinkedHashSet<>();
    this.typeInfos = readerContext.getTypeInfos();
    List<ColumnInfo> columnInfos = jobConf.getNecessaryOption(
        HBaseReaderOptions.COLUMNS, HBasePluginErrorCode.REQUIRED_VALUE);

    this.columnNames = columnInfos.stream().map(ColumnInfo::getName).collect(Collectors.toList());
    // Check if input column names are in format: [ columnFamily:column ].
    this.columnNames.stream().peek(column -> Preconditions.checkArgument(
            (column.contains(":") && column.split(":").length == 2) ||
                ROW_KEY.equalsIgnoreCase(column),
            "Invalid column names, it should be [ColumnFamily:Column] format"))
        .forEach(column -> columnFamilies.add(column.split(":")[0]));

    this.connection = HBaseHelper.getHbaseConnection(hbaseConfig);
    LOG.info("HBase source reader {} has connection created.", subTaskId);

    this.splits = new ConcurrentLinkedDeque<>();
    this.deserializationFormat = new HBaseDeserializationFormat(jobConf);
    this.deserializationSchema = deserializationFormat.createRuntimeDeserializationSchema(typeInfos);

    LOG.info("HBase source reader {} is initialized.", subTaskId);
  }

  @Override
  public void start() {
  }

  @Override
  public void pollNext(SourcePipeline<Row> pipeline) throws Exception {
    if (currentScanner == null && splits.isEmpty()) {
      return;
    }

    if (currentScanner == null) {
      this.currentSplit = splits.poll();

      Scan scan = new Scan();
      this.columnFamilies.forEach(cf -> scan.addFamily(Bytes.toBytes(cf)));
      this.currentScanner = connection.getTable(TableName.valueOf(this.tableName)).getScanner(scan);
    }
    Result result = this.currentScanner.next();
    if (result != null) {
      pipeline.output(deserializationSchema.deserialize(convertRawRow(result)));
    }
  }

  private byte[][] convertRawRow(Result result) {
    byte[][] rawRow = new byte[columnNames.size()][];
    for (int i = 0; i < columnNames.size(); ++i) {
      String columnName = columnNames.get(i);
      byte[] bytes;
      try {
        // If it is rowkey defined by users, directly use it.
        if (ROW_KEY.equals(columnName)) {
          bytes = result.getRow();
        } else {
          byte[][] arr = namesMap.get(columnName);
          // Deduplicate
          if (Objects.isNull(arr)) {
            arr = new byte[2][];
            String[] arr1 = columnName.split(":");
            arr[0] = arr1[0].trim().getBytes(StandardCharsets.UTF_8);
            arr[1] = arr1[1].trim().getBytes(StandardCharsets.UTF_8);
            namesMap.put(columnName, arr);
          }
          bytes = result.getValue(arr[0], arr[1]);
        }
        rawRow[i] = bytes;
      } catch (Exception e) {
        LOG.error("Cannot read data from {}, reason: \n", tableName, e);
      }
    }
    return rawRow;
  }

  @Override
  public void addSplits(List<HBaseSourceSplit> splitList) {
    totalSplitNum += splitList.size();
    splits.addAll(splitList);
  }

  @Override
  public boolean hasMoreElements() {
    if (hasNoMoreSplits && splits.isEmpty() && currentScanner == null) {
      LOG.info("Finish reading all {} splits.", totalSplitNum);
      return false;
    }
    return true;
  }

  @Override
  public void notifyNoMoreSplits() {
    hasNoMoreSplits = true;
    LOG.info("No more splits will be assigned.");
  }

  @Override
  public void handleSourceEvent(SourceEvent sourceEvent) {
    SourceReader.super.handleSourceEvent(sourceEvent);
  }

  @Override
  public List<HBaseSourceSplit> snapshotState(long checkpointId) {
    return null;
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) throws Exception {
    SourceReader.super.notifyCheckpointComplete(checkpointId);
  }

  @Override
  public void close() throws Exception {
    if (currentScanner != null) {
      try {
        currentScanner.close();
      } catch (Exception e) {
        throw new IOException("Failed to close HBase Scanner.", e);
      }
    }
    if (connection != null) {
      try {
        connection.close();
      } catch (Exception e) {
        throw new IOException("Failed to close HBase connection.", e);
      }
      LOG.info("Current HBase connection is closed.");
    }
  }

  public JobConf getConf() {
    JobConf jobConf = new JobConf(false);
    jobConf.set(TableInputFormat.INPUT_TABLE, tableName);
    hbaseConfig.forEach((key, value) -> jobConf.set(key, value.toString()));
    return jobConf;
  }
}
