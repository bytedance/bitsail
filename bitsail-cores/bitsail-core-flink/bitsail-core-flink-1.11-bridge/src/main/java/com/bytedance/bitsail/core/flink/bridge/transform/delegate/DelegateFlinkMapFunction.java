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

package com.bytedance.bitsail.core.flink.bridge.transform.delegate;

import com.bytedance.bitsail.base.connector.transform.MapFunctionType;
import com.bytedance.bitsail.base.connector.transform.v1.AppendStringMapFunction;
import com.bytedance.bitsail.base.connector.transform.v1.BitSailMapFunction;
import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.exception.CommonErrorCode;
import com.bytedance.bitsail.common.option.TransformOptions;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.common.typeinfo.RowTypeInfo;
import com.bytedance.bitsail.flink.core.delagate.converter.FlinkRowConvertSerializer;
import com.bytedance.bitsail.flink.core.typeutils.AutoDetectFlinkTypeInfoUtil;
import com.bytedance.bitsail.flink.core.typeutils.NativeFlinkTypeInfoUtil;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.util.List;
import java.util.Locale;

public class DelegateFlinkMapFunction<I, O extends org.apache.flink.types.Row> implements MapFunction<I, O> {

  private final BitSailMapFunction<com.bytedance.bitsail.common.row.Row, com.bytedance.bitsail.common.row.Row> realMapFunction;

  private final FlinkRowConvertSerializer flinkRowConvertSerializer;

  private final RowTypeInfo inputType;

  public DelegateFlinkMapFunction(BitSailConfiguration jobConf, TypeInformation<?> flinkTypes) {
    this.inputType = AutoDetectFlinkTypeInfoUtil.bridgeRowTypeInfo((org.apache.flink.api.java.typeutils.RowTypeInfo) flinkTypes);
    this.realMapFunction = createMapFunction(jobConf, inputType);
    this.flinkRowConvertSerializer = new FlinkRowConvertSerializer(
        this.inputType,
        jobConf);
  }

  @Override
  public O map(I value) throws Exception {
    org.apache.flink.types.Row outputRow;
    Row bitsailRow = flinkRowConvertSerializer.deserialize((org.apache.flink.types.Row) value);
    outputRow = flinkRowConvertSerializer.serialize(this.realMapFunction.map(bitsailRow));
    return (O) outputRow;
  }

  public TypeInformation getOutputType() {
    return NativeFlinkTypeInfoUtil.getRowTypeInformation(this.realMapFunction.getOutputType());
  }

  private BitSailMapFunction<Row, Row> createMapFunction(BitSailConfiguration jobConf, RowTypeInfo rowTypeInfo) {
    MapFunctionType mapFunctionType = MapFunctionType.valueOf(
        jobConf.get(TransformOptions.BaseTransformOptions.MAP_FUNCTION_TYPE).trim().toUpperCase(Locale.ROOT));
    switch (mapFunctionType) {
      case APPEND_STRING:
        List<String> columns = jobConf.getNecessaryOption(
            TransformOptions.BaseTransformOptions.APPEND_STRING_COLUMNS, CommonErrorCode.LACK_NECESSARY_FIELDS);
        List<String> values = jobConf.getNecessaryOption(
            TransformOptions.BaseTransformOptions.APPEND_STRING_VALUES, CommonErrorCode.LACK_NECESSARY_FIELDS);
        return new AppendStringMapFunction(columns, values, rowTypeInfo);
      default:
        throw BitSailException.asBitSailException(CommonErrorCode.TRANSFORM_ERROR,
            String.format("map function type %s is not supported yet", mapFunctionType));
    }
  }
}
