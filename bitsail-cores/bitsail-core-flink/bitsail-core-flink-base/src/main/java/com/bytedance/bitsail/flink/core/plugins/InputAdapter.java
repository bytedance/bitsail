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

package com.bytedance.bitsail.flink.core.plugins;

import com.bytedance.bitsail.base.messenger.common.MessengerGroup;
import com.bytedance.bitsail.common.BitSailException;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.types.Row;

/**
 * @class: InputAdapter
 * @desc:
 **/

public class InputAdapter extends AdapterPlugin {

  @Override
  public void initPlugin() {
    setBitSailTypeInfoFromFlink();
  }

  @Override
  public Row transform(Row bitSailRow, Row flinkRow) throws BitSailException {
    return getRowBytesParser().parseFlinkRow(bitSailRow, flinkRow, getBitSailRowTypeInfo());
  }

  @Override
  protected MessengerGroup getMessengerGroup() {
    return MessengerGroup.READER;
  }

  @Override
  public TypeInformation<?> getProducedType() {
    return getBitSailRowTypeInfo();
  }

  @Override
  public String getType() {
    return "InputAdapter";
  }
}
