/*
 *       Copyright 2022-2023 Bytedance Ltd. and/or its affiliates.
 *
 *       Licensed under the Apache License, Version 2.0 (the "License");
 *       you may not use this file except in compliance with the License.
 *       You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *       Unless required by applicable law or agreed to in writing, software
 *       distributed under the License is distributed on an "AS IS" BASIS,
 *       WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *       See the License for the specific language governing permissions and
 *       limitations under the License.
 */

package com.bytedance.bitsail.connector.kafka.format;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.typeinfo.RowTypeInfo;
import com.bytedance.bitsail.connector.kafka.constants.FormatType;
import com.bytedance.bitsail.connector.kafka.discoverer.PartitionDiscoverer;
import com.bytedance.bitsail.connector.kafka.option.KafkaOptions;

import org.apache.commons.lang3.StringUtils;

public class ProducerRecordSerializationSchemaFactory {

  public static ProducerRecordRowSerializationSchema<byte[], byte[]> getRowSerializationSchema(BitSailConfiguration jobConf,
                                                                                               RowTypeInfo rowTypeInfo,
                                                                                               PartitionDiscoverer partitionDiscoverer) {

    String topic = jobConf.get(KafkaOptions.TOPIC_NAME);
    FormatType formatType = FormatType.valueOf(StringUtils.upperCase(jobConf.get(KafkaOptions.FORMAT_TYPE)));
    String keyFields = jobConf.get(KafkaOptions.KEY_FIELDS);

    String partitionFields = jobConf.get(KafkaOptions.PARTITION_FIELD);
    if (StringUtils.isEmpty(partitionFields)) {
      return DefaultRowSerializationSchema.fixedTopic(
          formatType,
          topic,
          StringUtils.isEmpty(keyFields) ?
              null :
              StringUtils.split(keyFields, ","),
          rowTypeInfo,
          partitionDiscoverer,
          jobConf
      );
    }
    return DefaultRowSerializationSchema.fixedTopic(
        formatType,
        topic,
        StringUtils.isEmpty(keyFields) ?
            null :
            StringUtils.split(keyFields, ","),
        rowTypeInfo,
        partitionFields,
        partitionDiscoverer,
        jobConf
    );
  }
}
