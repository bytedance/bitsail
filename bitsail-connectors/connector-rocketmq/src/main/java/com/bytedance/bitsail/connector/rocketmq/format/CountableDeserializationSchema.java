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

public class CountableDeserializationSchema<IN, OUT> implements DeserializationSchema<IN, OUT> {

  private BitSailConfiguration deserializationConfiguration;
  private DeserializationSchema<IN, OUT> realDeserializationSchema;
  private int countNumber;
  private int current;

  @SuppressWarnings("checkstyle:MagicNumber")
  public CountableDeserializationSchema(BitSailConfiguration deserializationConfiguration,
                                        DeserializationSchema<IN, OUT> realDeserializationSchema) {
    this.deserializationConfiguration = deserializationConfiguration;
    this.realDeserializationSchema = realDeserializationSchema;
    this.countNumber = 100;
    this.current = 0;
  }

  @Override
  public OUT deserialize(IN message) {
    current++;
    return realDeserializationSchema.deserialize(message);
  }

  @Override
  public boolean isEndOfStream(OUT nextElement) {
    return current >= countNumber;
  }
}
