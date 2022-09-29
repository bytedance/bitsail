/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bytedance.bitsail.component.client.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;

/**
 * Simple KafkaProducer wrapper
 */
@Slf4j
public class KafkaProducer {
  private static final String CLUSTER_PROPERTY = "cluster";
  private static final String BROKER_PROPERTY = ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
  private static final int DEFAULT_BATCH_SIZE = 16384;
  private static final int DEFAULT_LINE_MS = 1000;
  private static final int DEFAULT_MEMORY_BUFFER = 33554432;

  private final org.apache.kafka.clients.producer.KafkaProducer<String, String> producer;

  private final String topic;
  private final int[] partitionList;

  public KafkaProducer(String cluster, String topic) {
    this(cluster, topic, null);
  }

  public KafkaProducer(String cluster, String topic, Map<String, Object> userConfigs) {
    this.topic = topic;

    Properties props = new Properties();

    log.info("Initializing Kafka cluster [{}], topic [{}].", cluster, topic);

    props.put(CLUSTER_PROPERTY, cluster);
    props.put(BROKER_PROPERTY, cluster);

    //Set it all (-1 equivalence) to get best consistence
    props.put(ProducerConfig.ACKS_CONFIG, "all");

    props.put(ProducerConfig.RETRIES_CONFIG, 0);

    //Set a suitable value to get a better performance
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, DEFAULT_BATCH_SIZE);

    // Send every 1s for better throughput
    props.put(ProducerConfig.LINGER_MS_CONFIG, DEFAULT_LINE_MS);

    props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, DEFAULT_MEMORY_BUFFER);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer");
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer");

    if (MapUtils.isNotEmpty(userConfigs)) {
      userConfigs.forEach(props::put);
    }

    producer = new org.apache.kafka.clients.producer.KafkaProducer<>(props);
    partitionList = getPartitionsByTopic(topic);
  }

  public static int choosePartitionIdByFields(int[] partitionList, String[] fields) {
    int totalFieldsHash = 0;
    for (String field : fields) {
      totalFieldsHash += field.hashCode();
    }

    int absTotalFieldsHash = Math.max(Math.abs(totalFieldsHash), 0);
    return partitionList[absTotalFieldsHash % partitionList.length];
  }

  public Future<RecordMetadata> send(String value) {
    // handle send exception?
    return producer.send(new ProducerRecord<>(topic, value));
  }

  public Future<RecordMetadata> send(String value, Callback callback) {
    return producer.send(new ProducerRecord<>(topic, value), callback);
  }

  public Future<RecordMetadata> send(String value, int partitionId) {
    // handle send exception?
    return producer.send(new ProducerRecord<>(topic, partitionId, null, value));
  }

  public Future<RecordMetadata> send(String value, int partitionId, Callback callback) {
    return producer.send(new ProducerRecord<>(topic, partitionId, null, value), callback);
  }

  public Future<RecordMetadata> send(String key, String value) {
    // handle send exception?
    return producer.send(new ProducerRecord<>(topic, key, value));
  }

  public Future<RecordMetadata> send(String key, String value, Callback callback) {
    return producer.send(new ProducerRecord<>(topic, key, value), callback);
  }

  /**
   * @return The partition ids array of target topic.
   */
  private int[] getPartitionsByTopic(String topic) {
    // the fetched list is immutable, so we're creating a mutable copy in order to sort it
    List<PartitionInfo> partitionsList = new ArrayList<>(producer.partitionsFor(topic));

    // sort the partitions by partition id to make sure the fetched partition list is the same across subtasks
    partitionsList.sort(Comparator.comparingInt(PartitionInfo::partition));

    int[] partitions = new int[partitionsList.size()];
    for (int i = 0; i < partitions.length; i++) {
      partitions[i] = partitionsList.get(i).partition();
    }

    return partitions;
  }

  public void close() {
    if (producer != null) {
      producer.flush();
      producer.close();
    }
  }
}
