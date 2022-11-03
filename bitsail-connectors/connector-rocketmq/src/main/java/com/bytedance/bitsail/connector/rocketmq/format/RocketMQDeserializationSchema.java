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

package com.bytedance.bitsail.connector.rocketmq.format;

import com.bytedance.bitsail.base.format.DeserializationSchema;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.common.typeinfo.TypeInfo;

public class RocketMQDeserializationSchema implements DeserializationSchema<byte[], Row> {

  private BitSailConfiguration deserializationConfiguration;

  private TypeInfo<?>[] typeInfos;

  public RocketMQDeserializationSchema(BitSailConfiguration deserializationConfiguration,
                                       TypeInfo<?>[] typeInfos) {
    this.deserializationConfiguration = deserializationConfiguration;
    this.typeInfos = typeInfos;
  }

  @Override
  public Row deserialize(byte[] message) {
    Row row = new Row(typeInfos.length);
    //todo deserialization
    return row;
  }

  @Override
  public boolean isEndOfStream(Row nextElement) {
    return false;
  }
}
