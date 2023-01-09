/*
 * Copyright 2022 Bytedance Ltd. and/or its affiliates.
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

package com.bytedance.bitsail.connector.hadoop.format;

import com.bytedance.bitsail.base.format.DeserializationSchema;
import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.model.ColumnInfo;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.common.typeinfo.TypeInfo;
import com.bytedance.bitsail.connector.hadoop.error.HadoopErrorCode;
import com.bytedance.bitsail.connector.hadoop.option.HadoopReaderOptions;

import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;

import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class HiveInputFormatDeserializationSchema implements DeserializationSchema<Writable, Row> {
  private final BitSailConfiguration deserializationConfiguration;
  private final TypeInfo<?>[] typeInfos;
  private final String[] fieldNames;
  private final StructObjectInspector inspector;
  public HiveInputFormatDeserializationSchema(BitSailConfiguration deserializationConfiguration,
                                                       TypeInfo<?>[] typeInfos,
                                                       String[] fieldNames) {

    this.deserializationConfiguration = deserializationConfiguration;
    this.typeInfos = typeInfos;
    this.fieldNames = fieldNames;

    List<ColumnInfo> columnInfos = deserializationConfiguration.get(HadoopReaderOptions.COLUMNS);
    Properties p = new Properties();
    String columns = columnInfos.stream().map(ColumnInfo::getName).collect(Collectors.joining(","));
    String columnsTypes = columnInfos.stream().map(ColumnInfo::getType).collect(Collectors.joining(":"));
    p.setProperty("columns", columns);
    p.setProperty("columns.types", columnsTypes);
    String inputFormatClass = deserializationConfiguration.get(HadoopReaderOptions.HADOOP_INPUT_FORMAT_CLASS);
    try {
      switch (inputFormatClass) {
        case "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat": {
          OrcSerde serde = new OrcSerde();
          serde.initialize(new JobConf(), p);
          this.inspector = (StructObjectInspector) serde.getObjectInspector();
          break;
        }
        case "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat": {
          ParquetHiveSerDe serde = new ParquetHiveSerDe();
          serde.initialize(new JobConf(), p);
          this.inspector = (StructObjectInspector) serde.getObjectInspector();
          break;
        }
        default:
          throw BitSailException.asBitSailException(HadoopErrorCode.UNSUPPORTED_ENCODING, "unsupported input format class: " + inputFormatClass);
      }
    } catch (SerDeException e) {
      throw BitSailException.asBitSailException(HadoopErrorCode.UNSUPPORTED_COLUMN_TYPE, "unsupported column information.");
    }
  }

  @Override
  public Row deserialize(Writable message) {
    int arity = fieldNames.length;
    List<? extends StructField> fields = inspector.getAllStructFieldRefs();
    Row row = new Row(arity);
    for (int i = 0; i < arity; ++i) {
      Object writableData = inspector.getStructFieldData(message, fields.get(i));
      row.setField(i, getWritableValue(writableData));
    }
    return row;
  }

  @Override
  public boolean isEndOfStream(Row nextElement) {
    return false;
  }

  private Object getWritableValue(Object writable) {
    Object ret;

    if (writable == null) {
      ret = null;
    } else if (writable instanceof IntWritable) {
      ret = ((IntWritable) writable).get();
    } else if (writable instanceof Text) {
      ret = writable.toString();
    } else if (writable instanceof LongWritable) {
      ret = ((LongWritable) writable).get();
    } else if (writable instanceof ByteWritable) {
      ret = ((ByteWritable) writable).get();
    } else if (writable instanceof DateWritable) {
      ret = ((DateWritable) writable).get();
    } else if (writable instanceof DoubleWritable) {
      ret = ((DoubleWritable) writable).get();
    } else if (writable instanceof TimestampWritable) {
      ret = ((TimestampWritable) writable).getTimestamp();
    } else if (writable instanceof FloatWritable) {
      ret = ((FloatWritable) writable).get();
    } else if (writable instanceof BooleanWritable) {
      ret = ((BooleanWritable) writable).get();
    } else if (writable instanceof BytesWritable) {
      BytesWritable bytesWritable = (BytesWritable) writable;
      byte[] bytes = bytesWritable.getBytes();
      ret = new byte[bytesWritable.getLength()];
      System.arraycopy(bytes, 0, ret, 0, bytesWritable.getLength());
    } else if (writable instanceof HiveDecimalWritable) {
      ret = ((HiveDecimalWritable) writable).getHiveDecimal().bigDecimalValue();
    } else if (writable instanceof ShortWritable) {
      ret = ((ShortWritable) writable).get();
    } else {
      ret = writable.toString();
    }
    return ret;
  }
}
