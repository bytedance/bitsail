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

package com.bytedance.bitsail.base.catalog;

import com.bytedance.bitsail.base.component.ComponentBuilder;
import com.bytedance.bitsail.base.connector.BuilderGroup;
import com.bytedance.bitsail.base.execution.ExecutionEnviron;
import com.bytedance.bitsail.common.catalog.table.TableCatalog;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;

import java.io.Serializable;

/**
 * Created 2022/5/23
 */
public interface TableCatalogFactory extends Serializable, ComponentBuilder {

  /**
   * Create a table catalog.
   *
   * @param executionEnviron       execution environment
   * @param connectorConfiguration configuration for the reader/writer
   */
  TableCatalog createTableCatalog(BuilderGroup builderGroup,
                                  ExecutionEnviron executionEnviron,
                                  BitSailConfiguration connectorConfiguration);

}
