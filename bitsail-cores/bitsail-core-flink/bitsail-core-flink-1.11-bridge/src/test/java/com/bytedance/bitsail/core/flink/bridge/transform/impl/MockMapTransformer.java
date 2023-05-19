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

package com.bytedance.bitsail.core.flink.bridge.transform.impl;

import com.bytedance.bitsail.base.connector.transform.v1.MapTransformer;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.common.typeinfo.RowTypeInfo;

public class MockMapTransformer implements MapTransformer<Row> {
  @Override
  public Row map(Row element) {
    return element;
  }

  @Override
  public void configure(BitSailConfiguration commonConfiguration, BitSailConfiguration transformConfiguration) {

  }

  @Override
  public void setTypeInfo(RowTypeInfo rowTypeInfo) {

  }

  @Override
  public String getComponentName() {
    return "MockMapTransformer";
  }
}
