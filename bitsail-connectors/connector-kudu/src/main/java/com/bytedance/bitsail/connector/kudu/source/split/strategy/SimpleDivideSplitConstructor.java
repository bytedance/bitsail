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
import com.bytedance.bitsail.common.model.ColumnInfo;
import com.bytedance.bitsail.connector.kudu.core.KuduFactory;
import com.bytedance.bitsail.connector.kudu.error.KuduErrorCode;
import com.bytedance.bitsail.connector.kudu.option.KuduReaderOptions;
import com.bytedance.bitsail.connector.kudu.source.split.KuduSourceSplit;
import com.bytedance.bitsail.connector.kudu.util.KuduPredicateUtil;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kudu.Schema;
import org.apache.kudu.client.AsyncKuduScanner;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduScanToken;
import org.apache.kudu.client.KuduTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class SimpleDivideSplitConstructor {
  private List<KuduSourceSplit> splits = null;
  private static final Logger LOG = LoggerFactory.getLogger(SimpleDivideSplitConstructor.class);

  protected final BitSailConfiguration jobConf;
  protected final String tableName;
  protected final Schema schema;
  protected final KuduTable kuduTable;
  private KuduClient kuduClient;

  private SimpleDivideSplitConstructor(BitSailConfiguration jobConf) {
    this.jobConf = jobConf;
    this.kuduClient = KuduFactory.initReaderFactory(jobConf).getClient();
    this.tableName = jobConf.get(KuduReaderOptions.KUDU_TABLE_NAME);
    try {
      this.kuduTable = kuduClient.openTable(tableName);
      this.schema = kuduTable.getSchema();
    } catch (KuduException e) {
      throw new BitSailException(KuduErrorCode.OPEN_TABLE_ERROR, "Failed to get schema from table " + tableName);
    }
  }

  public static SimpleDivideSplitConstructor getInstance(BitSailConfiguration jobConf) {
    return new SimpleDivideSplitConstructor(jobConf);
  }

  public int estimateSplitNum() {
    if (splits == null) {
      construct();
    }
    return splits.size();
  }

  public List<KuduSourceSplit> construct() {
    if (splits != null) {
      return splits;
    }
    KuduScanToken.KuduScanTokenBuilder tokenBuilder = kuduClient.newScanTokenBuilder(kuduTable);
    List<ColumnInfo> columnInfoConf = jobConf.get(KuduReaderOptions.COLUMNS);
    if (CollectionUtils.isNotEmpty(columnInfoConf)) {
      tokenBuilder.setProjectedColumnNames(columnInfoConf.stream().map(ColumnInfo::getName).collect(Collectors.toList()));
    }
    String predicateJson = jobConf.get(KuduReaderOptions.PREDICATES_CONFIGURATION);
    if (StringUtils.isNotEmpty(predicateJson)) {
      KuduPredicateUtil.parseFromConfig(predicateJson, schema).forEach(tokenBuilder::addPredicate);
    }
    String readMode = jobConf.get(KuduReaderOptions.READ_MODE);
    if (readMode != null) {
      tokenBuilder.readMode(AsyncKuduScanner.ReadMode.valueOf(readMode));
    }
    tokenBuilder.setFaultTolerant(jobConf.get(KuduReaderOptions.FAULT_TOLERANT));
    tokenBuilder.cacheBlocks(jobConf.get(KuduReaderOptions.CACHE_BLOCKS));
    tokenBuilder.scanRequestTimeout(jobConf.get(KuduReaderOptions.SCAN_TIMEOUT));
    Long snapshotTimestamp = jobConf.get(KuduReaderOptions.SNAPSHOT_TIMESTAMP_US);
    if (snapshotTimestamp != null) {
      tokenBuilder.snapshotTimestampMicros(snapshotTimestamp);
    }

    Integer scanBatchSize = jobConf.get(KuduReaderOptions.SCAN_BATCH_SIZE_BYTES);
    if (scanBatchSize != null) {
      tokenBuilder.batchSizeBytes(scanBatchSize);
    }

    Long scanMaxCount = jobConf.get(KuduReaderOptions.SCAN_MAX_COUNT);
    if (scanMaxCount != null) {
      tokenBuilder.limit(scanMaxCount);
    }

    Long scanAlivePeriodMs = jobConf.get(KuduReaderOptions.SCAN_ALIVE_PERIOD_MS);
    if (scanAlivePeriodMs != null) {
      tokenBuilder.keepAlivePeriodMs(scanAlivePeriodMs);
    }
    // control kudu source parallelism
    tokenBuilder.setSplitSizeBytes(jobConf.get(KuduReaderOptions.SCAN_SPLIT_SIZE_BYTES));

    List<KuduScanToken> tokens = tokenBuilder.build();
    splits = new ArrayList<>(tokens.size());
    for (KuduScanToken token : tokens) {
      List<String> locations = new ArrayList<>(token.getTablet().getReplicas().size());
      // support only leader in the future
      token.getTablet().getReplicas().forEach(x -> locations.add(x.getRpcHost()));
      KuduSourceSplit kuduSourceSplit = new KuduSourceSplit(token.hashCode());
      try {
        kuduSourceSplit.setSerializedScanToken(token.serialize());
      } catch (IOException e) {
        throw new BitSailException(KuduErrorCode.SPLIT_ERROR, "kudu scan token serialize error");
      }
      kuduSourceSplit.setLocations(locations.toArray(new String[0]));
      splits.add(kuduSourceSplit);
    }
    return splits;
  }
}
