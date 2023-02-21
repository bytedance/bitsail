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

package com.bytedance.bitsail.connector.redis.sink;

import com.bytedance.bitsail.base.connector.writer.v1.Sink;
import com.bytedance.bitsail.base.connector.writer.v1.Writer;
import com.bytedance.bitsail.base.connector.writer.v1.WriterCommitter;
import com.bytedance.bitsail.base.connector.writer.v1.state.EmptyState;
import com.bytedance.bitsail.base.serializer.BinarySerializer;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.common.type.BitSailTypeInfoConverter;
import com.bytedance.bitsail.common.type.TypeInfoConverter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Optional;

import static com.bytedance.bitsail.connector.redis.constant.RedisConstants.REDIS_CONNECTOR_NAME;

public class RedisSink<CommitT extends Serializable> implements Sink<Row, CommitT, EmptyState> {

  private static final Logger LOG = LoggerFactory.getLogger(RedisSink.class);

  private static final long serialVersionUID = -1;
  private BitSailConfiguration writerConf;

  @Override
  public String getWriterName() {
    return REDIS_CONNECTOR_NAME;
  }

  @Override
  public void configure(BitSailConfiguration commonConfiguration, BitSailConfiguration writerConfiguration) {
    writerConf = writerConfiguration;
  }

  @Override
  public Writer<Row, CommitT, EmptyState> createWriter(Writer.Context<EmptyState> context) {
    return new RedisWriter<>(writerConf, context);
  }

  @Override
  public TypeInfoConverter createTypeInfoConverter() {
    return new BitSailTypeInfoConverter();
  }

  @Override
  public Optional<WriterCommitter<CommitT>> createCommitter() {
    return Sink.super.createCommitter();
  }

  @Override
  public BinarySerializer<CommitT> getCommittableSerializer() {
    return Sink.super.getCommittableSerializer();
  }
}
