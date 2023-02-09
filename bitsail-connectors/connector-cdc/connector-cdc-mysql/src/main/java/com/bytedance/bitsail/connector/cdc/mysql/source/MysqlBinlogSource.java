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
import com.bytedance.bitsail.connector.cdc.mysql.source.reader.MysqlBinlogSourceReader;
import com.bytedance.bitsail.connector.cdc.source.BinlogSource;
import com.bytedance.bitsail.connector.cdc.source.split.BinlogSplit;

/**
 * WIP: Source to read mysql binlog.
 */
public class MysqlBinlogSource extends BinlogSource {

  @Override
  public SourceReader<Row, BinlogSplit> createReader(SourceReader.Context readerContext) {
    return new MysqlBinlogSourceReader(jobConf, readerContext);
  }

  @Override
  public String getReaderName() {
    return "mysql_cdc";
  }
}
