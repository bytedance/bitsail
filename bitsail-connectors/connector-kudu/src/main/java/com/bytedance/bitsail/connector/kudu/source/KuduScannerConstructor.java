/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.bytedance.bitsail.connector.kudu.source;

import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.model.ColumnInfo;
import com.bytedance.bitsail.connector.kudu.error.KuduErrorCode;
import com.bytedance.bitsail.connector.kudu.option.KuduReaderOptions;
import com.bytedance.bitsail.connector.kudu.source.split.KuduSourceSplit;

import org.apache.kudu.client.AsyncKuduScanner;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduScanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

public class KuduScannerConstructor implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(KuduScannerConstructor.class);

  private final List<String> projectedColumns;
  private final AsyncKuduScanner.ReadMode readMode;
  private final boolean enableFaultTolerant;
  private final boolean enableCacheBlocks;

  private final Long snapshotTimestamp;
  private final Integer scanBatchSize;
  private final Long scanMaxCount;
  private final Long scanTimeout;
  private final Long scanAlivePeriodMs;

  public KuduScannerConstructor(BitSailConfiguration jobConf) {
    List<ColumnInfo> columnInfos = jobConf.getNecessaryOption(KuduReaderOptions.COLUMNS, KuduErrorCode.REQUIRED_VALUE);
    this.projectedColumns = columnInfos.stream().map(ColumnInfo::getName).collect(Collectors.toList());

    this.snapshotTimestamp = jobConf.get(KuduReaderOptions.SNAPSHOT_TIMESTAMP_US);
    this.readMode = AsyncKuduScanner.ReadMode.valueOf(jobConf.get(KuduReaderOptions.READ_MODE));
    if (readMode.equals(AsyncKuduScanner.ReadMode.READ_YOUR_WRITES)) {
      throw new BitSailException(KuduErrorCode.CONFIG_ERROR, "Read mode READ_YOUR_WRITES is not supported.");
    }
    if (readMode.equals(AsyncKuduScanner.ReadMode.READ_AT_SNAPSHOT) && snapshotTimestamp == null) {
      throw new BitSailException(KuduErrorCode.CONFIG_ERROR, "Snapshort timestamp must be set when using READ_AT_SNAPSHOT read mode");
    }


    this.enableFaultTolerant = jobConf.get(KuduReaderOptions.FAULT_TOLERANT);
    this.scanBatchSize = jobConf.get(KuduReaderOptions.SCAN_BATCH_SIZE_BYTES);
    this.scanMaxCount = jobConf.get(KuduReaderOptions.SCAN_MAX_COUNT);
    this.enableCacheBlocks = jobConf.get(KuduReaderOptions.CACHE_BLOCKS);
    this.scanTimeout = jobConf.get(KuduReaderOptions.SCAN_TIMEOUT);
    this.scanAlivePeriodMs = jobConf.get(KuduReaderOptions.SCAN_ALIVE_PERIOD_MS);
  }

  public KuduScanner createScanner(KuduClient client, String tableName, KuduSourceSplit split) throws KuduException {
    KuduScanner.KuduScannerBuilder builder = client
        .newScannerBuilder(client.openTable(tableName))
        .setProjectedColumnNames(projectedColumns)
        .readMode(readMode)
        .setFaultTolerant(enableFaultTolerant)
        .cacheBlocks(enableCacheBlocks);

    if (snapshotTimestamp != null) {
      builder.snapshotTimestampMicros(snapshotTimestamp);
    }

    if (scanBatchSize != null) {
      builder.batchSizeBytes(scanBatchSize);
    }

    if (scanMaxCount != null) {
      builder.limit(scanMaxCount);
    }

    if (scanTimeout != null) {
      builder.scanRequestTimeout(scanTimeout);
    }

    if (scanAlivePeriodMs != null) {
      builder.keepAlivePeriodMs(scanAlivePeriodMs);
    }

    split.bindScanner(builder);

    KuduScanner scanner = builder.build();
    LOG.info("Scanner for split {} created.", split.uniqSplitId());
    return scanner;
  }
}