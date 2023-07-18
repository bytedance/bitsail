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

package com.bytedance.bitsail.connector.kudu.util;

import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.model.ColumnInfo;
import com.bytedance.bitsail.connector.kudu.error.KuduErrorCode;

import org.apache.kudu.Schema;
import org.apache.kudu.client.KuduTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class KuduSchemaUtils {
  private static final Logger LOG = LoggerFactory.getLogger(KuduSchemaUtils.class);

  /**
   * Check if all columns exist in kudu table to read or write.
   * @param kuduTable Kudu table to read or write.
   * @param columns Columns in configuration.
   */
  public static void checkColumnsExist(KuduTable kuduTable, List<ColumnInfo> columns) {
    if (columns == null) {
      return;
    }
    Schema schema = kuduTable.getSchema();

    boolean hasUnknownColumns = false;
    for (ColumnInfo column : columns) {
      String columnName = column.getName();
      try {
        if (schema.getColumn(columnName) == null) {
          LOG.error("Column {} does not exist in table {}.", columnName, kuduTable.getName());
          hasUnknownColumns = true;
        }
      } catch (Exception e) {
        LOG.error("Failed to find column {} from table.", columnName, e);
        hasUnknownColumns = true;
      }
    }

    if (hasUnknownColumns) {
      throw new BitSailException(KuduErrorCode.CONFIG_ERROR, "Found unknown column(s) in configuration.");
    }
  }
}

