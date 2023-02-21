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

package com.bytedance.bitsail.connector.rocketmq.format;

import com.bytedance.bitsail.base.format.DeserializationSchema;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.common.typeinfo.RowTypeInfo;
import com.bytedance.bitsail.component.format.json.JsonRowDeserializationSchema;

public class RocketMQDeserializationSchema implements DeserializationSchema<byte[], Row> {

  private BitSailConfiguration deserializationConfiguration;

  private transient JsonRowDeserializationSchema deserializationSchema;

  public RocketMQDeserializationSchema(BitSailConfiguration deserializationConfiguration,
                                       RowTypeInfo rowTypeInfo) {
    this.deserializationConfiguration = deserializationConfiguration;
    //todo spi.
    this.deserializationSchema = new JsonRowDeserializationSchema(deserializationConfiguration, rowTypeInfo);
  }

  @Override
  public Row deserialize(byte[] message) {
    return deserializationSchema.deserialize(message);
  }

  @Override
  public boolean isEndOfStream(Row nextElement) {
    return false;
  }
}
