/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.bytedance.bitsail.connector.legacy.hive.common;

import com.bytedance.bitsail.base.catalog.TableCatalogFactory;
import com.bytedance.bitsail.base.execution.ExecutionEnviron;
import com.bytedance.bitsail.common.catalog.table.TableCatalog;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;

import org.apache.hadoop.hive.conf.HiveConf;

public class HiveTableCatalogFactory implements TableCatalogFactory {

  private HiveConf hiveConf;

  public HiveTableCatalogFactory(HiveConf hiveConf) {
    this.hiveConf = hiveConf;
  }

  @Override
  public TableCatalog createTableCatalog(ExecutionEnviron executionEnviron,
                                         BitSailConfiguration connectorConfiguration) {
    return new HiveTableCatalog(
        executionEnviron.getCommonConfiguration(),
        connectorConfiguration,
        hiveConf);
  }

  @Override
  public String getComponentName() {
    return "hive";
  }
}
