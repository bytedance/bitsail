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

package com.bytedance.bitsail.connector.kafka.format;

import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.exception.CommonErrorCode;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.common.typeinfo.RowTypeInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfo;
import com.bytedance.bitsail.component.format.debezium.deserialization.DebeziumJsonDeserializationSchema;
import com.bytedance.bitsail.component.format.debezium.serialization.DebeziumJsonSerializationSchema;
import com.bytedance.bitsail.component.format.json.JsonRowSerializationSchema;
import com.bytedance.bitsail.connector.kafka.constants.FormatType;
import com.bytedance.bitsail.connector.kafka.discoverer.PartitionDiscoverer;

import lombok.AllArgsConstructor;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;

@AllArgsConstructor
public class DefaultRowSerializationSchema implements ProducerRecordRowSerializationSchema<byte[], byte[]> {

  private final Function<Row, String> topicFunction;
  private final Function<Row, byte[]> keyFunction;
  private final Function<Row, byte[]> valueFunction;
  private final Function<Row, Integer> partitionFunction;

  public static DefaultRowSerializationSchema fixedTopic(FormatType format,
                                                         String topic,
                                                         String[] keyFieldNames,
                                                         RowTypeInfo rowTypeInfo,
                                                         PartitionDiscoverer partitionDiscoverer,
                                                         BitSailConfiguration jobConf) {
    return new DefaultRowSerializationSchema(
        (row) -> topic,
        createKeyFunction(keyFieldNames, format, rowTypeInfo, jobConf),
        createValueFunction(format, rowTypeInfo, jobConf),
        createPartitionFunction(format, rowTypeInfo, null, partitionDiscoverer)
    );
  }

  public static DefaultRowSerializationSchema fixedTopic(FormatType format,
                                                         String topic,
                                                         String[] keyFieldNames,
                                                         RowTypeInfo rowTypeInfo,
                                                         String partitionField,
                                                         PartitionDiscoverer partitionDiscoverer,
                                                         BitSailConfiguration jobConf) {
    return new DefaultRowSerializationSchema(
        (row) -> topic,
        createKeyFunction(keyFieldNames, format, rowTypeInfo, jobConf),
        createValueFunction(format, rowTypeInfo, jobConf),
        createPartitionFunction(format, rowTypeInfo, partitionField, partitionDiscoverer)
    );
  }

  private static Function<Row, byte[]> createValueFunction(FormatType format, RowTypeInfo rowTypeInfo, BitSailConfiguration jobConf) {
    if (FormatType.JSON.equals(format)) {
      JsonRowSerializationSchema jsonRowSerializationSchema = new JsonRowSerializationSchema(jobConf, rowTypeInfo);
      return jsonRowSerializationSchema::serialize;
    }
    if (FormatType.DEBEZIUM_JSON.equals(format)) {
      DebeziumJsonSerializationSchema serializationSchema = new DebeziumJsonSerializationSchema(rowTypeInfo, DebeziumJsonDeserializationSchema.VALUE_NAME);
      return serializationSchema::serialize;
    }
    throw BitSailException.asBitSailException(CommonErrorCode.CONFIG_ERROR,
        String.format("Format type %s not support.", format));
  }

  private static Function<Row, byte[]> createKeyFunction(String[] keyFiledNames,
                                                         FormatType format,
                                                         RowTypeInfo rowTypeInfo,
                                                         BitSailConfiguration jobConf) {
    if (FormatType.DEBEZIUM_JSON.equals(format)) {
      DebeziumJsonSerializationSchema serializationSchema = new DebeziumJsonSerializationSchema(rowTypeInfo, DebeziumJsonDeserializationSchema.KEY_NAME);
      return serializationSchema::serialize;
    }

    if (ArrayUtils.isEmpty(keyFiledNames)) {
      return row -> null;
    }

    RowTypeInfo slice = slice(rowTypeInfo, keyFiledNames);
    if (FormatType.JSON.equals(format)) {
      JsonRowSerializationSchema jsonRowSerializationSchema = new JsonRowSerializationSchema(jobConf, slice);
      return jsonRowSerializationSchema::serialize;
    }

    throw BitSailException.asBitSailException(CommonErrorCode.CONFIG_ERROR,
        String.format("Format type %s not support.", format));
  }

  private static Function<Row, Integer> createPartitionFunction(FormatType format,
                                                                RowTypeInfo rowTypeInfo,
                                                                String partitionFields,
                                                                PartitionDiscoverer partitionDiscoverer) {
    if (FormatType.DEBEZIUM_JSON.equals(format)) {
      //default use topic field hash value.
      final List<PartitionInfo> partitionInfos = partitionDiscoverer.discoverPartitions();
      int i = rowTypeInfo.indexOf(DebeziumJsonDeserializationSchema.TOPIC_NAME);
      return (row -> Math.abs(row.getField(i).hashCode()) % partitionInfos.size());
    }

    if (StringUtils.isEmpty(partitionFields)) {
      return row -> null;
    }

    if (FormatType.JSON.equals(format)) {
      return createFixedPartitionFunction(StringUtils.split(partitionFields, ","), rowTypeInfo, partitionDiscoverer);
    }

    throw BitSailException.asBitSailException(CommonErrorCode.CONFIG_ERROR,
        String.format("Format type %s not support for partition function", format));
  }

  private static Function<Row, Integer> createFixedPartitionFunction(String[] partitionFields,
                                                                     RowTypeInfo rowTypeInfo,
                                                                     PartitionDiscoverer partitionDiscoverer) {
    final int[] partitionFieldIndices = new int[partitionFields.length];
    List<PartitionInfo> partitionInfos = partitionDiscoverer.discoverPartitions();
    for (int index = 0; index < partitionFields.length; index++) {
      int indexOf = rowTypeInfo.indexOf(partitionFields[index]);
      if (indexOf < 0) {
        throw BitSailException.asBitSailException(CommonErrorCode.CONFIG_ERROR, String.format("Partition field %s not exists.", partitionFields[index]));
      }
      partitionFieldIndices[index] = indexOf;
    }

    return row -> {
      int hashcode = 0;
      for (int partitionFieldIndex : partitionFieldIndices) {
        Object field = row.getField(partitionFieldIndex);
        hashcode = hashcode + Objects.hashCode(field);
      }
      return Math.abs(hashcode) % partitionInfos.size();
    };
  }

  private static RowTypeInfo slice(RowTypeInfo rowTypeInfo, String[] keyFiledNames) {
    TypeInfo<?>[] slice = new TypeInfo<?>[keyFiledNames.length];
    for (int index = 0; index < keyFiledNames.length; index++) {
      int indexOf = rowTypeInfo.indexOf(keyFiledNames[index]);
      slice[index] = rowTypeInfo.getTypeInfos()[indexOf];
    }
    return new RowTypeInfo(keyFiledNames, slice);
  }

  @Override
  public ProducerRecord<byte[], byte[]> serialize(Row row) {
    return new ProducerRecord<byte[], byte[]>(
        topicFunction.apply(row),
        partitionFunction.apply(row),
        keyFunction.apply(row),
        valueFunction.apply(row)
    );
  }
}
