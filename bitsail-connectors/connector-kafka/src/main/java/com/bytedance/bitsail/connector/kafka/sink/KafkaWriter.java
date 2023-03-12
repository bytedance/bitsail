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
import com.bytedance.bitsail.common.model.ColumnInfo;
import com.bytedance.bitsail.common.option.CommonOptions;
import com.bytedance.bitsail.common.row.BinlogRow;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.common.typeinfo.TypeInfo;
import com.bytedance.bitsail.component.format.json.RowToJsonConverter;
import com.bytedance.bitsail.connector.kafka.common.KafkaErrorCode;
import com.bytedance.bitsail.connector.kafka.model.KafkaRecord;
import com.bytedance.bitsail.connector.kafka.option.KafkaWriterOptions;

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
  private final String bootstrapServers;

  private final KafkaProducer kafkaProducer;
  /**
   * The INDEX list of partition fields in columns
   */
  private List<Integer> partitionFieldsIndices;

  private final transient Writer.Context context;

  private final TypeInfo<?>[] typeInfos;

  //TODO: move fieldNames to writer context
  private final List<String> fieldNames;

  private final String format;

  private final RowToJsonConverter jsonConverter;

  public KafkaWriter(BitSailConfiguration commonConf, BitSailConfiguration writerConf, Context<EmptyState> context) {
    this.context = context;
    List<ColumnInfo> columns = writerConf.getNecessaryOption(KafkaWriterOptions.COLUMNS, KafkaErrorCode.REQUIRED_VALUE);
    this.typeInfos = context.getRowTypeInfo().getTypeInfos();
    this.fieldNames = columns.stream().map(ColumnInfo::getName).collect(Collectors.toList());
    this.jsonConverter = new RowToJsonConverter(context.getRowTypeInfo());
    this.bootstrapServers = writerConf.getNecessaryOption(KafkaWriterOptions.BOOTSTRAP_SERVERS, KafkaErrorCode.REQUIRED_VALUE);
    this.kafkaTopic = writerConf.getNecessaryOption(KafkaWriterOptions.TOPIC_NAME, KafkaErrorCode.REQUIRED_VALUE);
    this.format = writerConf.getNecessaryOption(KafkaWriterOptions.CONTENT_TYPE, KafkaErrorCode.REQUIRED_VALUE);

    logFailuresOnly = writerConf.get(KafkaWriterOptions.LOG_FAILURES_ONLY);

    optionalConfig = commonConf.getUnNecessaryOption(CommonOptions.OPTIONAL, new HashMap<>());
    addDefaultProducerParams(optionalConfig, writerConf);
    LOG.info("Kafka producer optional config is: " + optionalConfig);

    String partitionField = writerConf.get(KafkaWriterOptions.PARTITION_FIELD);
    if (StringUtils.isNotEmpty(partitionField)) {
      List<String> partitionFieldsNames = Arrays.asList(partitionField.split(",\\s*"));
      partitionFieldsIndices = getPartitionFieldsIndices(fieldNames, partitionFieldsNames);
    }

    if (format.equals("debezium")) {
      optionalConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
    }
    this.kafkaProducer = new KafkaProducer(this.bootstrapServers, this.kafkaTopic, optionalConfig);
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
    //TODO: refactor this as a format factory
    if (format.equals("debezium")) {
      writeDebezium(record);
    } else {
      String result = jsonConverter.convert(record).toString();
      // get partition id to insert if 'partitionFieldsIndices' is not empty
      if (CollectionUtils.isNotEmpty(partitionFieldsIndices)) {
        String[] partitionFieldsValues = new String[partitionFieldsIndices.size()];
        for (int i = 0; i < partitionFieldsIndices.size(); i++) {
          int partitionFieldIndex = partitionFieldsIndices.get(i);
          partitionFieldsValues[i] = String.valueOf(record.getField(partitionFieldIndex));
        }
        int partitionId = choosePartitionIdByFields(partitionFieldsValues);
        sendByPartitionId(result, partitionId);
      } else {
        send(result);
      }
    }
  }

  @SuppressWarnings("checkstyle:MagicNumber")
  public void writeDebezium(Row record) {
    String[] partitionFieldsValues = new String[1];
    String key = record.getString(BinlogRow.KEY_INDEX);
    partitionFieldsValues[0] = key;
    int partitionId = choosePartitionIdByFields(partitionFieldsValues);
    Map<String, String> headers = new HashMap<>(4);
    headers.put("db", record.getString(BinlogRow.DATABASE_INDEX));
    headers.put("table", record.getString(BinlogRow.TABLE_INDEX));
    headers.put("ddl_flag", String.valueOf(record.getBoolean(BinlogRow.DDL_FLAG_INDEX)));
    headers.put("version", String.valueOf(record.getInt(BinlogRow.VERSION_INDEX)));
    byte[] value = record.getBinary(BinlogRow.VALUE_INDEX);
    sendWithHeaders(key, value, partitionId, headers);
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

  private void send(Object value) {
    kafkaProducer.send(KafkaRecord.builder().value(value).build(), callback);
  }

  private void send(String key, Object value) {
    kafkaProducer.send(KafkaRecord.builder().key(key).value(value).build(), callback);
  }

  private void sendByPartitionId(Object value, int partitionId) {
    kafkaProducer.send(KafkaRecord.builder().value(value).partitionId(partitionId).build(), callback);
  }

  private void sendByPartitionId(String key, Object value, int partitionId) {
    kafkaProducer.send(KafkaRecord.builder().key(key).value(value).partitionId(partitionId).build(), callback);
  }

  private void sendWithHeaders(String value, Map<String, String> headers) {
    kafkaProducer.send(KafkaRecord.builder().value(value).headers(headers).build(), callback);
  }

  private void sendWithHeaders(String key, Object value, Map<String, String> headers) {
    kafkaProducer.send(KafkaRecord.builder().key(key).value(value).headers(headers).build(), callback);
  }

  private void sendWithHeaders(String key, Object value, int partitionId, Map<String, String> headers) {
    kafkaProducer.send(KafkaRecord.builder().key(key).value(value).partitionId(partitionId).headers(headers).build(), callback);
  }

  private void closeProducer() {
    if (Objects.nonNull(kafkaProducer)) {
      kafkaProducer.close();
    }
  }
}
