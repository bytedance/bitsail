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

package com.bytedance.bitsail.connector.kafka.discoverer;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

public class DefaultPartitionDiscoverer implements PartitionDiscoverer {

  private final Properties properties;
  private final String topic;
  private final int indexOfThisSubtask;
  private final KafkaConsumer<?, ?> consumer;

  public DefaultPartitionDiscoverer(Properties properties,
                                    String topic,
                                    int indexOfThisSubtask) {
    this.properties = addMoreProperties(properties);
    this.topic = topic;
    this.indexOfThisSubtask = indexOfThisSubtask;
    this.consumer = new KafkaConsumer<Object, Object>(this.properties);
  }

  private static Properties addMoreProperties(Properties properties) {
    Properties newProperties = new Properties();
    newProperties.putAll(properties);
    newProperties.put(KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    newProperties.put(VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

    return newProperties;
  }

  @Override
  public List<PartitionInfo> discoverPartitions() {
    return consumer.partitionsFor(topic);
  }

  @Override
  public void close() throws IOException {
    if (Objects.nonNull(consumer)) {
      consumer.close();
    }
  }
}
