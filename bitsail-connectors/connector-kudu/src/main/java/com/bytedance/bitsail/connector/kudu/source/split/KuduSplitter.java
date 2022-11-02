/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.bytedance.bitsail.connector.kudu.source.split;

import com.bytedance.bitsail.base.connector.reader.v1.SourceSplit;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.model.ColumnInfo;
import com.bytedance.bitsail.connector.kudu.option.KuduReaderOptions;

import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduPredicate;
import org.apache.kudu.client.KuduScanToken.KuduScanTokenBuilder;
import org.apache.kudu.client.KuduTable;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class KuduSplitter {

  private final String tableName;
  private final List<ColumnInfo> columns;

  private final boolean enableCacheBlocks;
  private final boolean enableFaultTolerant;
  private final long scanTimeout;

  public KuduSplitter(BitSailConfiguration jobConf) {
    this.tableName = jobConf.get(KuduReaderOptions.KUDU_TABLE_NAME);
    this.columns = jobConf.get(KuduReaderOptions.COLUMNS);

    this.enableCacheBlocks = jobConf.get(KuduReaderOptions.CACHE_BLOCKS);
    this.enableFaultTolerant = jobConf.get(KuduReaderOptions.FAULT_TOLERANT);
    this.scanTimeout = jobConf.get(KuduReaderOptions.SCAN_TIMEOUT);
  }

  public List<SourceSplit> getSplits(KuduClient kuduClient) throws IOException {
    // step0: open table
    KuduTable kuduTable;
    try {
      kuduTable = kuduClient.openTable(tableName);
      if (kuduTable == null) {
        throw new IOException("Table not found.");
      }
    } catch (KuduException e) {
      throw new IOException("Table " + tableName + " not found.");
    }

    // step1: build scan token
    List<String> projectedColumns = columns.stream().map(ColumnInfo::getName).collect(Collectors.toList());
    KuduScanTokenBuilder tokenBuilder = kuduClient.newScanTokenBuilder(kuduTable)
        .setProjectedColumnNames(projectedColumns)
        .cacheBlocks(enableCacheBlocks)
        .setFaultTolerant(enableFaultTolerant)
        .setTimeout(scanTimeout);

    KuduPredicate.deserialize()
  }
}
