/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bytedance.bitsail.common.ddl;

import com.bytedance.bitsail.common.model.ColumnInfo;
import com.bytedance.bitsail.common.type.BaseEngineTypeInfoConverter;

import java.io.Serializable;
import java.util.List;

public interface ExternalEngineConnector extends Serializable {

  /**
   * Get external engine system's name for type system.
   */
  String getExternalEngineName();

  /**
   * Acquire all external columns by connect to external system.
   */
  List<ColumnInfo> getExternalColumnInfos() throws Exception;

  /**
   * Create converter for the external engine.
   */
  BaseEngineTypeInfoConverter createTypeInfoConverter();
}
