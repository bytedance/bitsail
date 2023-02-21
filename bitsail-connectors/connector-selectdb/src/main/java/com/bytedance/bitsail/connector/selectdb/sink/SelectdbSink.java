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

package com.bytedance.bitsail.connector.selectdb.sink;

import com.bytedance.bitsail.base.connector.writer.v1.Sink;
import com.bytedance.bitsail.base.connector.writer.v1.Writer;
import com.bytedance.bitsail.base.connector.writer.v1.WriterCommitter;
import com.bytedance.bitsail.base.serializer.BinarySerializer;
import com.bytedance.bitsail.base.serializer.SimpleVersionedBinarySerializer;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.model.ColumnInfo;
import com.bytedance.bitsail.common.type.TypeInfoConverter;
import com.bytedance.bitsail.common.type.filemapping.FileMappingTypeInfoConverter;
import com.bytedance.bitsail.connector.selectdb.committer.SelectdbCommittable;
import com.bytedance.bitsail.connector.selectdb.committer.SelectdbCommittableSerializer;
import com.bytedance.bitsail.connector.selectdb.committer.SelectdbCommitter;
import com.bytedance.bitsail.connector.selectdb.config.SelectdbExecutionOptions;
import com.bytedance.bitsail.connector.selectdb.config.SelectdbOptions;
import com.bytedance.bitsail.connector.selectdb.config.SelectdbWriterOptions;
import com.bytedance.bitsail.connector.selectdb.error.SelectdbErrorCode;

import com.alibaba.fastjson.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

public class SelectdbSink<InputT> implements Sink<InputT, SelectdbCommittable, SelectdbWriterState> {
  private static final Logger LOG = LoggerFactory.getLogger(SelectdbSink.class);
  private SelectdbOptions selectdbOptions;
  private SelectdbExecutionOptions selectdbExecutionOptions;
  private BitSailConfiguration writerConfiguration;

  @Override
  public String getWriterName() {
    return "selectdb";
  }

  @Override
  public void configure(BitSailConfiguration commonConfiguration, BitSailConfiguration writerConfiguration) throws IOException {
    this.writerConfiguration = writerConfiguration;
    initSelectdbExecutionOptions(writerConfiguration);
    initSelectdbOptions(writerConfiguration);
  }

  @Override
  public Writer<InputT, SelectdbCommittable, SelectdbWriterState> createWriter(Writer.Context<SelectdbWriterState> context) {
    return new SelectdbWriter(writerConfiguration, selectdbOptions, selectdbExecutionOptions);
  }

  @Override
  public TypeInfoConverter createTypeInfoConverter() {
    return new FileMappingTypeInfoConverter(getWriterName());
  }

  @Override
  public Optional<WriterCommitter<SelectdbCommittable>> createCommitter() {
    return Optional.of(new SelectdbCommitter(selectdbOptions, selectdbExecutionOptions));
  }

  @Override
  public BinarySerializer<SelectdbCommittable> getCommittableSerializer() {
    return new SelectdbCommittableSerializer();
  }

  @Override
  public BinarySerializer<SelectdbWriterState> getWriteStateSerializer() {
    return new SimpleVersionedBinarySerializer<SelectdbWriterState>();
  }

  private void initSelectdbOptions(BitSailConfiguration writerConfiguration) {
    LOG.info("Start to init SelectdbOptions!");
    SelectdbOptions.SelectdbOptionsBuilder builder =
        SelectdbOptions.builder().clusterName(writerConfiguration.getNecessaryOption(SelectdbWriterOptions.CLUSTER_NAME, SelectdbErrorCode.REQUIRED_VALUE))
            .loadUrl(writerConfiguration.getNecessaryOption(SelectdbWriterOptions.LOAD_URL, SelectdbErrorCode.REQUIRED_VALUE))
            .tableIdentifier(writerConfiguration.getNecessaryOption(SelectdbWriterOptions.TABLE_IDENTIFIER, SelectdbErrorCode.REQUIRED_VALUE))
            .username(writerConfiguration.getNecessaryOption(SelectdbWriterOptions.USER, SelectdbErrorCode.REQUIRED_VALUE))
            .password(writerConfiguration.getNecessaryOption(SelectdbWriterOptions.PASSWORD, SelectdbErrorCode.REQUIRED_VALUE))
            .fieldDelimiter(writerConfiguration.get(SelectdbWriterOptions.CSV_FIELD_DELIMITER))
            .lineDelimiter(writerConfiguration.get(SelectdbWriterOptions.CSV_LINE_DELIMITER))
            .loadDataFormat(SelectdbOptions.LOAD_CONTENT_TYPE.valueOf(writerConfiguration.get(SelectdbWriterOptions.LOAD_CONTEND_TYPE).toUpperCase()))
            .columnInfos(JSON.parseArray(writerConfiguration.getString(SelectdbWriterOptions.COLUMNS.key()), ColumnInfo.class));

    selectdbOptions = builder.build();
  }

  private void initSelectdbExecutionOptions(BitSailConfiguration writerConfiguration) {
    LOG.info("Start to init SelectdbExecutionOptions!");
    final SelectdbExecutionOptions.SelectdbExecutionOptionsBuilder builder = SelectdbExecutionOptions.builder();
    builder.flushIntervalMs(writerConfiguration.get(SelectdbWriterOptions.SINK_FLUSH_INTERVAL_MS))
        .maxRetries(writerConfiguration.get(SelectdbWriterOptions.SINK_MAX_RETRIES)).bufferCount(writerConfiguration.get(SelectdbWriterOptions.SINK_BUFFER_COUNT))
        .bufferSize(writerConfiguration.get(SelectdbWriterOptions.SINK_BUFFER_SIZE)).labelPrefix(writerConfiguration.get(SelectdbWriterOptions.SINK_LABEL_PREFIX))
        .enableDelete(writerConfiguration.get(SelectdbWriterOptions.SINK_ENABLE_DELETE))
        .writerMode(SelectdbExecutionOptions.WRITE_MODE.valueOf(writerConfiguration.get(SelectdbWriterOptions.SINK_WRITE_MODE)));

    Map<String, String> streamProperties = writerConfiguration.getUnNecessaryOption(SelectdbWriterOptions.STREAM_LOAD_PROPERTIES, new HashMap<>());
    Properties streamLoadProp = new Properties();
    streamLoadProp.putAll(streamProperties);
    builder.streamLoadProp(streamLoadProp);
    selectdbExecutionOptions = builder.build();
  }
}

