/*
 * Copyright [2022] [ByteDance]
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

package com.bytedance.bitsail.connector.legacy.rocketmq.sink.format;

import org.apache.flink.types.Row;

import java.io.Serializable;

public interface RocketMQSerializationSchema extends Serializable {

  byte[] serializeKey(Row row);

  byte[] serializeValue(Row row);

  default String getPartitionKey(Row row) {
    return null;
  }
}
