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

package com.bytedance.bitsail.connector.kafka.sink;

import com.bytedance.bitsail.connector.kafka.sink.callback.CallbackWrapper;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

public class KafkaSender implements Closeable {

  private final KafkaProducer<byte[], byte[]> producer;

  public KafkaSender(Properties properties) {
    this.producer = new KafkaProducer<byte[], byte[]>(properties);
  }

  public void send(ProducerRecord<byte[], byte[]> producerRecord,
                   CallbackWrapper callbackWrapper) {
    producer.send(producerRecord, callbackWrapper);
  }

  public void flush() {
    producer.flush();
  }

  @Override
  public void close() throws IOException {
    producer.close();
  }

  public List<PartitionInfo> partitionFor(String topic) {
    return producer.partitionsFor(topic);
  }
}
