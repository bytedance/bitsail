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

package com.bytedance.bitsail.connector.elasticsearch.sink;

import com.bytedance.bitsail.base.connector.writer.v1.Writer;
import com.bytedance.bitsail.base.connector.writer.v1.WriterCommitter;
import com.bytedance.bitsail.base.connector.writer.v1.WriterGenerator;
import com.bytedance.bitsail.base.connector.writer.v1.state.EmptyState;
import com.bytedance.bitsail.base.serializer.BinarySerializer;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.common.type.TypeInfoConverter;
import com.bytedance.bitsail.common.type.filemapping.FileMappingTypeInfoConverter;
import com.bytedance.bitsail.connector.elasticsearch.base.EsConstants;

import java.util.List;
import java.util.Optional;

public class ElasticsearchWriterGenerator<CommitT> implements WriterGenerator<Row, CommitT, EmptyState> {

  private BitSailConfiguration writerConf;

  @Override
  public String getWriterName() {
    return EsConstants.ES_CONNECTOR_NAME;
  }

  @Override
  public void configure(BitSailConfiguration commonConfiguration, BitSailConfiguration writerConfiguration) {
    writerConf = writerConfiguration;
  }

  @Override
  public Writer<Row, CommitT, EmptyState> createWriter(BitSailConfiguration writerConfiguration, Writer.Context context) {
    return new ElasticsearchWriter<>(writerConf);
  }

  @Override
  public Writer<Row, CommitT, EmptyState> restoreWriter(BitSailConfiguration writerConfiguration,
                                                        List<EmptyState> writerStates,
                                                        Writer.Context context) {
    return createWriter(writerConf, context);
  }

  @Override
  public TypeInfoConverter createTypeInfoConverter() {
    return new FileMappingTypeInfoConverter(getWriterName());
  }

  @Override
  public Optional<WriterCommitter<CommitT>> createCommitter() {
    return Optional.empty();
  }

  @Override
  public Optional<BinarySerializer<CommitT>> getCommittableSerializer() {
    return Optional.empty();
  }

  @Override
  public Optional<BinarySerializer<EmptyState>> getWriteStateSerializer() {
    BinarySerializer<EmptyState> serializer = new BinarySerializer<EmptyState>() {
      @Override
      public byte[] serialize(EmptyState obj) {
        return new byte[0];
      }

      @Override
      public EmptyState deserialize(byte[] serialized) {
        return EmptyState.fromBytes();
      }
    };
    return Optional.of(serializer);
  }
}
