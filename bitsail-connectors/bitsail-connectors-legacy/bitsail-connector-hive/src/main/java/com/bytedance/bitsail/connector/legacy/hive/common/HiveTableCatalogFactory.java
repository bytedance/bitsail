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

package com.bytedance.bitsail.connector.legacy.hive.common;

import com.bytedance.bitsail.base.catalog.TableCatalogFactory;
import com.bytedance.bitsail.base.connector.BuilderGroup;
import com.bytedance.bitsail.base.execution.ExecutionEnviron;
import com.bytedance.bitsail.common.catalog.table.TableCatalog;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.exception.FrameworkErrorCode;
import com.bytedance.bitsail.connector.legacy.hive.option.HiveReaderOptions;
import com.bytedance.bitsail.connector.legacy.hive.option.HiveWriterOptions;
import com.bytedance.bitsail.connector.legacy.hive.util.HiveConfUtils;

public class HiveTableCatalogFactory implements TableCatalogFactory {

  @Override
  public TableCatalog createTableCatalog(BuilderGroup builderGroup,
                                         ExecutionEnviron executionEnviron,
                                         BitSailConfiguration connectorConfiguration) {
    if (BuilderGroup.READER.equals(builderGroup)) {
      String database = connectorConfiguration
          .getNecessaryOption(HiveReaderOptions.DB_NAME, FrameworkErrorCode.REQUIRED_VALUE);
      String table = connectorConfiguration
          .getNecessaryOption(HiveReaderOptions.TABLE_NAME, FrameworkErrorCode.REQUIRED_VALUE);
      return HiveTableCatalog
          .builder()
          .database(database)
          .table(table)
          .namespace(null)
          .hiveConf(HiveConfUtils.fromJsonProperties(
              connectorConfiguration.get(HiveReaderOptions.HIVE_METASTORE_PROPERTIES)))
          .build();
    } else {
      String database = connectorConfiguration
          .getNecessaryOption(HiveWriterOptions.DB_NAME, FrameworkErrorCode.REQUIRED_VALUE);
      String table = connectorConfiguration
          .getNecessaryOption(HiveWriterOptions.TABLE_NAME, FrameworkErrorCode.REQUIRED_VALUE);
      return HiveTableCatalog
          .builder()
          .database(database)
          .table(table)
          .namespace(null)
          .hiveConf(HiveConfUtils.fromJsonProperties(
              connectorConfiguration.get(HiveWriterOptions.HIVE_METASTORE_PROPERTIES)))
          .build();
    }
  }

  @Override
  public String getComponentName() {
    return "hive";
  }
}
