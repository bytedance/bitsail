/*
 * Copyright 2022-2023 Bytedance Ltd. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.bytedance.bitsail.connector.cdc.sqlserver.source;

import com.bytedance.bitsail.base.connector.reader.v1.SourceReader;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.connector.cdc.source.BaseCDCSource;
import com.bytedance.bitsail.connector.cdc.source.split.BaseCDCSplit;
import com.bytedance.bitsail.connector.cdc.source.split.BaseSplitSerializer;
import com.bytedance.bitsail.connector.cdc.sqlserver.source.reader.SqlServerCDCSourceReader;
import com.bytedance.bitsail.connector.cdc.sqlserver.source.split.SqlServerSplitSerializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SqlServerCDCSource extends BaseCDCSource {
  private static final Logger LOG = LoggerFactory.getLogger(SqlServerCDCSource.class);

  @Override
  public SourceReader<Row, BaseCDCSplit> createReader(SourceReader.Context readerContext) {
    LOG.info("Create SqlServer CDC Source");
    return new SqlServerCDCSourceReader(readerConf, commonConf, readerContext, deserializationSchema, boundedness);
  }

  @Override
  public BaseSplitSerializer createSplitSerializer() {
    return new SqlServerSplitSerializer();
  }

  @Override
  public String getReaderName() {
    return "sqlserver_cdc";
  }
}
