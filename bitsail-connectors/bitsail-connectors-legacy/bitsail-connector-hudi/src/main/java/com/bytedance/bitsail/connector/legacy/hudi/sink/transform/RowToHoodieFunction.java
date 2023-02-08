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

package com.bytedance.bitsail.connector.legacy.hudi.sink.transform;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.model.ColumnInfo;
import com.bytedance.bitsail.common.option.WriterOptions;
import com.bytedance.bitsail.connector.legacy.hudi.sink.utils.PayloadCreation;
import com.bytedance.bitsail.connector.legacy.hudi.util.RowToAvroConverters;
import com.bytedance.bitsail.connector.legacy.hudi.util.StreamerUtil;
import com.bytedance.bitsail.flink.core.typeutils.NativeFlinkTypeInfoUtil;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.keygen.KeyGenerator;
import org.apache.hudi.keygen.factory.HoodieAvroKeyGeneratorFactory;

import java.io.IOException;
import java.util.List;

public class RowToHoodieFunction<I extends Row, O extends HoodieRecord>
    extends RichMapFunction<I, O> {
  /**
   * Config options.
   */
  private final Configuration config;

  private final BitSailConfiguration jobConf;
  /**
   * Avro schema of the input.
   */
  private transient Schema avroSchema;
  /**
   * RowData to Avro record converter.
   */
  private transient RowToAvroConverters.RowToAvroConverter converter;
  /**
   * HoodieKey generator.
   */
  private transient KeyGenerator keyGenerator;
  /**
   * Utilities to create hoodie pay load instance.
   */
  private transient PayloadCreation payloadCreation;

  public RowToHoodieFunction(Configuration config, BitSailConfiguration jobConf) {
    this.config = config;
    this.jobConf = jobConf;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    this.avroSchema = StreamerUtil.getSourceSchema(this.config);
    List<ColumnInfo> columnInfos = jobConf.get(WriterOptions.BaseWriterOptions.COLUMNS);
    RowTypeInfo rowTypeInfo = NativeFlinkTypeInfoUtil.getRowTypeInformation(columnInfos);
    this.converter = RowToAvroConverters.createConverter(rowTypeInfo);
    this.keyGenerator =
        HoodieAvroKeyGeneratorFactory
            .createKeyGenerator(StreamerUtil.flinkConf2TypedProperties(this.config));
    this.payloadCreation = PayloadCreation.instance(config);
  }

  @SuppressWarnings("unchecked")
  @Override
  public O map(I i) throws Exception {
    return (O) toHoodieRecord(i);
  }

  /**
   * Converts the give record to a {@link HoodieRecord}.
   *
   * @param record The input record
   * @return HoodieRecord based on the configuration
   * @throws IOException if error occurs
   */
  @SuppressWarnings("rawtypes")
  private HoodieRecord toHoodieRecord(I record) throws Exception {
    GenericRecord gr = (GenericRecord) this.converter.convert(this.avroSchema, record);
    final HoodieKey hoodieKey = keyGenerator.getKey(gr);

    HoodieRecordPayload payload = payloadCreation.createPayload(gr);
    //TODO: Support CDC
    HoodieOperation operation = HoodieOperation.INSERT;
    return new HoodieAvroRecord<>(hoodieKey, payload, operation);
  }
}
