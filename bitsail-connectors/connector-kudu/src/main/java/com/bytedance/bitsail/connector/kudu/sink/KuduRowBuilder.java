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

package com.bytedance.bitsail.connector.kudu.sink;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.model.ColumnInfo;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.connector.kudu.option.KuduWriterOptions;

import org.apache.kudu.client.PartialRow;

import java.io.Serializable;
import java.util.List;

public class KuduRowBuilder implements Serializable {

  private final List<ColumnInfo> columnInfos;

  public KuduRowBuilder(BitSailConfiguration jobConf) {
    this.columnInfos = jobConf.get(KuduWriterOptions.COLUMNS);
  }

  public void transform(PartialRow kuduRow, Row row) {
    Object[] fields = row.getFields();

    String columnName;
    for (int i = 0; i < fields.length; ++i) {
       columnName = columnInfos.get(i).getName();

    }
  }
}
