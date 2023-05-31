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

package com.bytedance.bitsail.transforms.map.encrypt;

import com.bytedance.bitsail.base.connector.transform.v1.MapTransformer;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.common.typeinfo.RowTypeInfo;

import org.apache.commons.lang3.StringUtils;

public class EncryptFieldMapTransformer implements MapTransformer<Row> {
  private BitSailConfiguration transformConfiguration;
  private RowTypeInfo rowTypeInfo;
  private int fieldIndex;
  private String fieldName;
  private Encrypts encrypts;

  @Override
  public void configure(BitSailConfiguration commonConfiguration, BitSailConfiguration transformConfiguration) {
    this.transformConfiguration = transformConfiguration;
    this.fieldName = transformConfiguration.get(EncryptOptions.FIELD_NAME);
    this.encrypts = Encrypts.valueOf(StringUtils.upperCase(transformConfiguration.get(EncryptOptions.ENCRYPT_NAME)));
  }

  @Override
  public void setTypeInfo(RowTypeInfo rowTypeInfo) {
    this.rowTypeInfo = rowTypeInfo;
    this.fieldIndex = rowTypeInfo.indexOf(fieldName);
  }

  @Override
  public Row map(Row element) {
    Object field = element.getField(fieldIndex);
    element.setField(fieldIndex, encrypts.encrypt(field == null ? null : field.toString()));
    return element;
  }

  @Override
  public String getComponentName() {
    return "EncryptFieldMapTransformer";
  }
}
