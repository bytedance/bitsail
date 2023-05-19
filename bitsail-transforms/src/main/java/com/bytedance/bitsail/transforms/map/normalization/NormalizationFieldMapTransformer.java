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

package com.bytedance.bitsail.transforms.map.normalization;

import com.bytedance.bitsail.base.connector.transform.v1.MapTransformer;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.common.typeinfo.RowTypeInfo;

import java.util.List;

public class NormalizationFieldMapTransformer implements MapTransformer<Row> {
  private BitSailConfiguration transformConfiguration;
  private List<String> appendStringColumns;
  private List<String> appendStringValues;
  private RowTypeInfo rowTypeInfo;

  @Override
  public void configure(BitSailConfiguration commonConfiguration, BitSailConfiguration transformConfiguration) {
    this.transformConfiguration = transformConfiguration;
  }

  @Override
  public void setTypeInfo(RowTypeInfo rowTypeInfo) {
    this.rowTypeInfo = rowTypeInfo;
  }

  @Override
  public Row map(Row element) {
    return element;
  }

  @Override
  public String getComponentName() {
    return "Normalization";
  }
}
