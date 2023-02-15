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

package com.bytedance.bitsail.connector.kafka.sink;

import com.bytedance.bitsail.base.connector.writer.v1.Writer;
import com.bytedance.bitsail.base.connector.writer.v1.state.EmptyState;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.option.CommonOptions;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.common.typeinfo.TypeInfo;
import com.bytedance.bitsail.connector.kafka.common.KafkaErrorCode;
import com.bytedance.bitsail.connector.kafka.model.KafkaRecord;
import com.bytedance.bitsail.connector.kafka.option.KafkaWriterOptions;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.bytedance.bitsail.connector.kafka.constants.KafkaConstants.REQUEST_TIMEOUT_MS_CONFIG;

public class KafkaWriter<CommitT> implements Writer<Row, CommitT, EmptyState> {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaWriter.class);

  protected Map<String, Object> optionalConfig;
  /**
   * For handling error propagation or logging callbacks.
   */
  protected transient Callback callback;
  /**
   * Errors encountered in the async producer are stored here.
   */
  protected transient volatile Exception asyncException;
  /**
   * When encountering kafka send failures, log the exception or throw it anyway.
   */
  protected boolean logFailuresOnly;
  private final String kafkaTopic;
  private final String kafkaServers;

  private final KafkaProducer kafkaProducer;
  /**
   * The INDEX list of partition fields in columns
   */
  private List<Integer> partitionFieldsIndices;

  private final transient Writer.Context context;

  private final TypeInfo<?>[] typeInfos;

  //TODO: move fieldNames to writer context
  private final List<String> fieldNames;

  public KafkaWriter(BitSailConfiguration commonConf, BitSailConfiguration writerConf, Context<EmptyState> context) {
    this.context = context;
    this.typeInfos = context.getRowTypeInfo().getTypeInfos();
    this.fieldNames = Lists.newArrayList(context.getRowTypeInfo().getFieldNames());
    this.kafkaServers = writerConf.getNecessaryOption(KafkaWriterOptions.KAFKA_SERVERS, KafkaErrorCode.REQUIRED_VALUE);
    this.kafkaTopic = writerConf.getNecessaryOption(KafkaWriterOptions.TOPIC_NAME, KafkaErrorCode.REQUIRED_VALUE);

    logFailuresOnly = writerConf.get(KafkaWriterOptions.LOG_FAILURES_ONLY);

    optionalConfig = commonConf.getUnNecessaryOption(CommonOptions.OPTIONAL, new HashMap<>());
    addDefaultProducerParams(optionalConfig, writerConf);
    LOG.info("Kafka producer optional config is: " + optionalConfig);

    String partitionField = writerConf.get(KafkaWriterOptions.PARTITION_FIELD);
    if (StringUtils.isNotEmpty(partitionField)) {
      List<String> partitionFieldsNames = Arrays.asList(partitionField.split(",\\s*"));
      partitionFieldsIndices = getPartitionFieldsIndices(fieldNames, partitionFieldsNames);
    }

    this.kafkaProducer = new KafkaProducer(this.kafkaServers, this.kafkaTopic, optionalConfig);
    if (logFailuresOnly) {
      callback = (metadata, e) -> {
        if (e != null) {
          LOG.error("Error while sending record to Kafka: " + e.getMessage(), e);
        }
      };
    } else {
      callback = (metadata, exception) -> {
        if (exception != null && asyncException == null) {
          asyncException = exception;
        }
      };
    }
  }

  @Override
  public void write(Row record) throws IOException {
    checkErroneous();
    JSONObject jsonObject = new JSONObject();
    String columnName;
    Object columnValue;
    for (int i = 0; i < record.getArity(); i++) {
      columnName = fieldNames.get(i);
      columnValue = record.getField(i);
      jsonObject.put(columnName, columnValue);
    }
    // get partition id to insert if 'partitionFieldsIndices' is not empty
    if (CollectionUtils.isNotEmpty(partitionFieldsIndices)) {
      String[] partitionFieldsValues = new String[partitionFieldsIndices.size()];
      for (int i = 0; i < partitionFieldsIndices.size(); i++) {
        int partitionFieldIndex = partitionFieldsIndices.get(i);
        partitionFieldsValues[i] = String.valueOf(record.getField(partitionFieldIndex));
      }
      int partitionId = choosePartitionIdByFields(partitionFieldsValues);
      sendByPartitionId(jsonObject.toJSONString(), partitionId);
    } else {
      send(jsonObject.toJSONString());
    }
  }

  @Override
  public void flush(boolean endOfInput) throws IOException {
    synchronized (this) {
      checkErroneous();
      if (Objects.nonNull(kafkaProducer)) {
        kafkaProducer.flush();
      }
    }
  }

  @Override
  public List<CommitT> prepareCommit() throws IOException {
    return Collections.emptyList();
  }

  @Override
  public List<EmptyState> snapshotState(long checkpointId) throws IOException {
    this.flush(false);
    return Collections.emptyList();
  }

  @Override
  public void close() throws IOException {
    closeProducer();
    checkErroneous();
  }

  private void addDefaultProducerParams(Map<String, Object> optionalConfig, BitSailConfiguration writerConf) {
    if (!optionalConfig.containsKey(ProducerConfig.RETRIES_CONFIG)) {
      optionalConfig.put(ProducerConfig.RETRIES_CONFIG, writerConf.get(KafkaWriterOptions.RETRIES));
    }
    if (!optionalConfig.containsKey(ProducerConfig.RETRY_BACKOFF_MS_CONFIG)) {
      optionalConfig.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, writerConf.get(KafkaWriterOptions.RETRY_BACKOFF_MS));
    }
    if (!optionalConfig.containsKey(ProducerConfig.LINGER_MS_CONFIG)) {
      optionalConfig.put(ProducerConfig.LINGER_MS_CONFIG, writerConf.get(KafkaWriterOptions.LINGER_MS));
    }
    if (!optionalConfig.containsKey(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG)) {
      optionalConfig.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, REQUEST_TIMEOUT_MS_CONFIG);
    }
  }

  /**
   * get all partition fields indices according to their name
   */
  private List<Integer> getPartitionFieldsIndices(List<String> fieldNames, List<String> partitionFieldsNames) {
    return partitionFieldsNames.stream()
        .map(partitionFieldsName -> {
          for (int i = 0; i < fieldNames.size(); i++) {
            String columnName = fieldNames.get(i);
            if (columnName.equals(partitionFieldsName)) {
              return i;
            }
          }
          throw new IllegalArgumentException("partitionFieldsNames is not in columns");
        }).collect(Collectors.toList());
  }

  protected void checkErroneous() throws IOException {
    Exception e = asyncException;
    if (e != null) {
      // in case double throwing
      asyncException = null;
      throw new IOException("Failed to send data to Kafka: " + e.getMessage(), e);
    }
  }

  private int choosePartitionIdByFields(String[] fields) {
    return kafkaProducer.choosePartitionIdByFields(fields);
  }

  private void send(KafkaRecord record) {
    kafkaProducer.send(record, callback);
  }

  private void send(String value) {
    kafkaProducer.send(KafkaRecord.builder().value(value).build(), callback);
  }

  private void send(String key, String value) {
    kafkaProducer.send(KafkaRecord.builder().key(key).value(value).build(), callback);
  }

  private void sendByPartitionId(String value, int partitionId) {
    kafkaProducer.send(KafkaRecord.builder().value(value).partitionId(partitionId).build(), callback);
  }

  private void sendByPartitionId(String key, String value, int partitionId) {
    kafkaProducer.send(KafkaRecord.builder().key(key).value(value).partitionId(partitionId).build(), callback);
  }

  private void sendWithHeaders(String value, Map<String, String> headers) {
    kafkaProducer.send(KafkaRecord.builder().value(value).headers(headers).build(), callback);
  }

  private void sendWithHeaders(String key, String value, Map<String, String> headers) {
    kafkaProducer.send(KafkaRecord.builder().key(key).value(value).headers(headers).build(), callback);
  }

  private void sendWithHeaders(String key, String value, int partitionId, Map<String, String> headers) {
    kafkaProducer.send(KafkaRecord.builder().key(key).value(value).partitionId(partitionId).headers(headers).build(), callback);
  }

  private void closeProducer() {
    if (Objects.nonNull(kafkaProducer)) {
      kafkaProducer.close();
    }
  }
}
