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

package com.bytedance.bitsail.connector.cdc.mysql.source;

import com.bytedance.bitsail.base.connector.reader.v1.SourceReader;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.connector.cdc.mysql.source.reader.MysqlCDCSourceReader;
import com.bytedance.bitsail.connector.cdc.mysql.source.split.MysqlSplitSerializer;
import com.bytedance.bitsail.connector.cdc.source.BaseCDCSource;
import com.bytedance.bitsail.connector.cdc.source.split.BaseCDCSplit;
import com.bytedance.bitsail.connector.cdc.source.split.BaseSplitSerializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Source to read mysql binlog.
 */
public class MysqlCDCSource extends BaseCDCSource {

  private static final Logger LOG = LoggerFactory.getLogger(MysqlCDCSource.class);
  @Override
  public SourceReader<Row, BaseCDCSplit> createReader(SourceReader.Context readerContext) {
    LOG.info("Create Mysql CDC Source");
    return new MysqlCDCSourceReader(readerConf, commonConf, readerContext, deserializationSchema, boundedness);
  }

  @Override
  public BaseSplitSerializer createSplitSerializer() {
    return new MysqlSplitSerializer();
  }

  @Override
  public String getReaderName() {
    return "mysql_cdc";
  }
}
