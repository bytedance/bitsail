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

import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.typeinfo.RowTypeInfo;
import com.bytedance.bitsail.connector.rocketmq.error.RocketMQErrorCode;
import com.bytedance.bitsail.connector.rocketmq.sink.format.RocketMQSinkFormat;

import java.util.List;

public class RocketMQSerializationFactory {

  RowTypeInfo rowTypeInfo;
  List<Integer> partitionIndices;
  List<Integer> keyIndices;

  public RocketMQSerializationFactory(RowTypeInfo rowTypeInfo, List<Integer> partitionIndices, List<Integer> keyIndices) {
    this.rowTypeInfo = rowTypeInfo;
    this.partitionIndices = partitionIndices;
    this.keyIndices = keyIndices;
  }

  public RocketMQSerializationSchema getSerializationSchemaByFormat(BitSailConfiguration bitSailConfiguration, RocketMQSinkFormat format) {
    if (format == RocketMQSinkFormat.JSON) {
      return new JsonRocketMQSerializationSchema(bitSailConfiguration, rowTypeInfo, partitionIndices, keyIndices);
    }
    throw BitSailException.asBitSailException(RocketMQErrorCode.UNSUPPORTED_FORMAT,
        "unsupported sink format: " + format);
  }
}
