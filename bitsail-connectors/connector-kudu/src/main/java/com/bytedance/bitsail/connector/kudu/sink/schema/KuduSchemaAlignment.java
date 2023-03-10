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

package com.bytedance.bitsail.connector.kudu.sink.schema;

import com.bytedance.bitsail.base.extension.SchemaAlignmentable;
import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.connector.kudu.error.KuduErrorCode;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.client.AlterTableOptions;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduTable;

import java.util.List;

public class KuduSchemaAlignment implements SchemaAlignmentable<List<ColumnSchema>> {
  private final transient KuduClient kuduClient;
  private final transient KuduTable kuduTable;

  public KuduSchemaAlignment(KuduClient kuduClient, KuduTable kuduTable) {
    this.kuduClient = kuduClient;
    this.kuduTable = kuduTable;
  }

  @Override
  public void align(List<ColumnSchema> currentColumnSchema,  List<ColumnSchema> targetColumnSchema) {
    try {
      AlterTableOptions alterTableOptions = new AlterTableOptions();
      for (ColumnSchema newColumn : targetColumnSchema) {
        if (currentColumnSchema.stream().noneMatch(column -> column.getName().equals(newColumn.getName()))) {
          alterTableOptions.addColumn(newColumn);
        }
      }
      kuduClient.alterTable(kuduTable.getName(), alterTableOptions);
    } catch (Exception e) {
      throw BitSailException.asBitSailException(KuduErrorCode.ALTER_TABLE_ERROR, e);
    }
  }
}
