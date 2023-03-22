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

package com.bytedance.bitsail.connector.rocketmq.sink;

import com.bytedance.bitsail.base.connector.writer.v1.Writer;
import com.bytedance.bitsail.base.connector.writer.v1.state.EmptyState;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.model.ColumnInfo;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.common.util.Preconditions;
import com.bytedance.bitsail.connector.rocketmq.error.RocketMQErrorCode;
import com.bytedance.bitsail.connector.rocketmq.format.RocketMQSerializationFactory;
import com.bytedance.bitsail.connector.rocketmq.format.RocketMQSerializationSchema;
import com.bytedance.bitsail.connector.rocketmq.option.RocketMQWriterOptions;
import com.bytedance.bitsail.connector.rocketmq.sink.config.RocketMQSinkConfig;
import com.bytedance.bitsail.connector.rocketmq.sink.format.RocketMQSinkFormat;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class RocketMQWriter<CommitT> implements Writer<Row, CommitT, EmptyState> {
  private static final Logger LOG = LoggerFactory.getLogger(RocketMQWriter.class);
  
  private final RocketMQProducer rocketmqProducer;

  private final String topic;
  private final String tag;

  private final List<Integer> partitionIndices;

  private final RocketMQSerializationSchema serializationSchema;

  public RocketMQWriter(BitSailConfiguration commonConf, BitSailConfiguration writerConf,
                        Context<EmptyState> context) throws IOException {
    RocketMQSinkConfig sinkConfig = new RocketMQSinkConfig(writerConf);
    this.topic = sinkConfig.getTopic();
    this.tag = sinkConfig.getTag();
    this.rocketmqProducer = new RocketMQProducer(sinkConfig);

    List<ColumnInfo> columns = writerConf.getNecessaryOption(RocketMQWriterOptions.COLUMNS,
        RocketMQErrorCode.REQUIRED_VALUE);

    String partitionFields = writerConf.getNecessaryOption(RocketMQWriterOptions.PARTITION_FIELDS, RocketMQErrorCode.REQUIRED_VALUE);
    this.partitionIndices = getIndicesByFieldNames(columns, partitionFields);

    String keyFields = writerConf.getNecessaryOption(RocketMQWriterOptions.KEY_FIELDS, RocketMQErrorCode.REQUIRED_VALUE);
    List<Integer> keyIndices = getIndicesByFieldNames(columns, keyFields);
    this.open();

    String formatType = writerConf.get(RocketMQWriterOptions.FORMAT);
    RocketMQSinkFormat sinkFormat = RocketMQSinkFormat.valueOf(formatType.toUpperCase());

    LOG.info("RocketMQ producer settings: " + sinkConfig);
    LOG.info("RocketMQ partition fields indices: " + partitionIndices);
    LOG.info("RocketMQ key indices: " + keyIndices);
    LOG.info("RocketMQ sink format type: " + sinkFormat);

    RocketMQSerializationFactory factory = new RocketMQSerializationFactory(context.getRowTypeInfo(), partitionIndices, keyIndices);
    this.serializationSchema = factory.getSerializationSchemaByFormat(writerConf, sinkFormat);
  }

  public void open() throws IOException {
    if (partitionIndices != null && !partitionIndices.isEmpty()) {
      rocketmqProducer.setEnableQueueSelector(true);
    }
    rocketmqProducer.validateParams();

    try {
      this.rocketmqProducer.open();
    } catch (Exception e) {
      throw new IOException("failed to open rocketmq producer: " + e.getMessage(), e);
    }
  }

  @Override
  public void write(Row row) throws IOException {
    Message message = prepareMessage(row);
    String partitionKeys = serializationSchema.getPartitionKey(row);
    try {
      rocketmqProducer.send(message, partitionKeys);
    } catch (Exception e) {
      throw new IOException("failed to send record to rocketmq: " + e.getMessage(), e);
    }
  }

  @Override
  public void flush(boolean endOfInput) throws IOException {
    synchronized (this) {
      if (Objects.nonNull(rocketmqProducer)) {
        rocketmqProducer.flushMessages(!endOfInput);
      }
      if (endOfInput) {
        LOG.info("all records are sent to commit buffer.");
      }
    }
  }

  @Override
  public List <CommitT> prepareCommit() throws IOException {
    return null;
  }

  /**
   * transform BitSail Row to RocketMQ Message (value, topic, tag)
   */
  private Message prepareMessage(Row row) {
    byte[] k = serializationSchema.serializeKey(row);
    byte[] value = serializationSchema.serializeValue(row);
    String key = k != null ? new String(k, StandardCharsets.UTF_8) : "";

    Preconditions.checkNotNull(topic, "the message topic is null");
    Preconditions.checkNotNull(value, "the message body is null");

    Message msg = new Message(topic, value);
    msg.setKeys(key);
    msg.setTags(tag);

    return msg;
  }

  /**
   * get indices by field names
   */
  private List<Integer> getIndicesByFieldNames(List<ColumnInfo> columns, String fieldNames) {
    if (StringUtils.isEmpty(fieldNames)) {
      return null;
    }

    List<String> fields = Arrays.asList(fieldNames.split(",\\s*"));
    List<Integer> indices = fields.stream().map(field -> {
      for (int i = 0; i < columns.size(); ++i) {
        String columnName = columns.get(i).getName().trim();
        if (columnName.equals(field)) {
          return i;
        }
      }
      throw new IllegalArgumentException("Field " + field + " not found in columns! All fields are: " + fieldNames);
    }).collect(Collectors.toList());
    return indices.isEmpty() ? null : indices;
  }

}
