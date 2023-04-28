/*
 *       Copyright 2022-2023 Bytedance Ltd. and/or its affiliates.
 *
 *       Licensed under the Apache License, Version 2.0 (the "License");
 *       you may not use this file except in compliance with the License.
 *       You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *       Unless required by applicable law or agreed to in writing, software
 *       distributed under the License is distributed on an "AS IS" BASIS,
 *       WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *       See the License for the specific language governing permissions and
 *       limitations under the License.
 */

package com.bytedance.bitsail.connector.cdc.util;

import com.bytedance.bitsail.common.catalog.table.TableId;
import com.bytedance.bitsail.common.row.MultipleTableRow;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import static io.debezium.data.Envelope.FieldName.SOURCE;

public class MultipleTableRowUtils {

  public static MultipleTableRow fromSourceRecord(SourceRecord record, byte[] serialized) {
    Struct value = (Struct) record.value();
    TableId tableId = TableId
        .of(value.getStruct(SOURCE).getString("db"), value.getStruct(SOURCE).getString("table"));
    return MultipleTableRow
        .of(tableId.toString(),
            null,
            new String(serialized),
            StringUtils.EMPTY,
            StringUtils.EMPTY);
  }
}
