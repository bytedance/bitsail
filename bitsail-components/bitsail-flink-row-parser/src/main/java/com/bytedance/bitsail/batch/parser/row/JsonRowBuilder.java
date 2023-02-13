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

package com.bytedance.bitsail.batch.parser.row;

import com.bytedance.bitsail.batch.file.parser.JsonBytesParser;
import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.component.format.api.RowBuilder;
import com.bytedance.bitsail.flink.core.parser.BytesParser;
import com.bytedance.bitsail.parser.error.ParserErrorCode;

import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

public class JsonRowBuilder<T> implements RowBuilder<T> {

  private BytesParser bytesParser;

  JsonRowBuilder(BitSailConfiguration inputSliceConfig) throws Exception {
    // es client return json response
    this.bytesParser = new JsonBytesParser(inputSliceConfig);
  }

  @Override
  public void build(T value, Row reuse, RowTypeInfo rowTypeInfo) throws BitSailException {
    try {
      bytesParser.parse(reuse, value, rowTypeInfo);
    } catch (Exception e) {
      throw BitSailException.asBitSailException(ParserErrorCode.ILLEGAL_JSON, "value: " + value.toString(), e);
    }
  }
}
