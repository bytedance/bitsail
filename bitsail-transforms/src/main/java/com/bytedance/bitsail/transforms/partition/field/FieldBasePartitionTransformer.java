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

package com.bytedance.bitsail.transforms.partition.field;

import com.bytedance.bitsail.base.connector.transform.v1.PartitionTransformer;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.common.typeinfo.RowTypeInfo;

import java.util.Objects;

public class FieldBasePartitionTransformer implements PartitionTransformer<Row, Object> {

  private BitSailConfiguration commonConfiguration;
  private BitSailConfiguration transformConfiguration;
  private String fieldName;
  private RowTypeInfo rowTypeInfo;
  private int fieldIndex;

  @Override
  public void configure(BitSailConfiguration commonConfiguration, BitSailConfiguration transformConfiguration) {
    this.commonConfiguration = commonConfiguration;
    this.transformConfiguration = transformConfiguration;
    this.fieldName = transformConfiguration.get(FieldBaseOptions.FIELD_NAME);
  }

  @Override
  public void setTypeInfo(RowTypeInfo rowTypeInfo) {
    this.rowTypeInfo = rowTypeInfo;
    this.fieldIndex = rowTypeInfo.indexOf(fieldName);
  }

  @Override
  public Object selectKey(Row element) {
    return element.getField(fieldIndex);
  }

  @Override
  public int partition(Object key, int totalPartitions) {
    return Math.abs(Objects.hash(key)) % totalPartitions;
  }

  @Override
  public String getComponentName() {
    return "FieldBasePartitioner";
  }
}
