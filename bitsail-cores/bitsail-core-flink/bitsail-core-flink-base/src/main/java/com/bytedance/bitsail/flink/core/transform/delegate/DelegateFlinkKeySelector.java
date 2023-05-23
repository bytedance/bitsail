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

package com.bytedance.bitsail.flink.core.transform.delegate;

import com.bytedance.bitsail.base.connector.transform.v1.PartitionTransformer;
import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.common.typeinfo.RowTypeInfo;
import com.bytedance.bitsail.flink.core.delagate.converter.FlinkRowConverter;

import org.apache.flink.api.java.functions.KeySelector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DelegateFlinkKeySelector<Input1 extends org.apache.flink.types.Row, Input2 extends Row, Key> implements KeySelector<Input1, Key> {
  private static final Logger LOG = LoggerFactory.getLogger(DelegateFlinkKeySelector.class);

  private final BitSailConfiguration commonConfiguration;
  private final PartitionTransformer<Input2, Key> partitionTransformer;
  private final RowTypeInfo rowTypeInfo;
  private volatile boolean initialized;
  private transient FlinkRowConverter rowConverter;

  public DelegateFlinkKeySelector(BitSailConfiguration commonConfiguration,
                                  PartitionTransformer<Input2, Key> partitionTransformer,
                                  RowTypeInfo rowTypeInfo) {
    this.commonConfiguration = commonConfiguration;
    this.partitionTransformer = partitionTransformer;
    this.rowTypeInfo = rowTypeInfo;
  }

  private void initialize() {
    if (!initialized) {
      rowConverter = new FlinkRowConverter(rowTypeInfo, commonConfiguration);
      initialized = true;
    }
  }

  @Override
  public Key getKey(Input1 input) throws Exception {
    initialize();
    Row converted;
    try {
      converted = rowConverter.from(input);
    } catch (BitSailException e) {
      LOG.error("Transform {} convert input failed.", partitionTransformer.getComponentName(), e);
      //TODO dirty collect
      throw new IllegalArgumentException(e);
    }

    return partitionTransformer.selectKey((Input2) converted);
  }
}
