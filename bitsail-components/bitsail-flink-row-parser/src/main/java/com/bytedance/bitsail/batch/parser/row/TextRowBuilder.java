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

import com.bytedance.bitsail.base.enumerate.ContentType;
import com.bytedance.bitsail.batch.file.parser.BytesParseFactory;
import com.bytedance.bitsail.batch.file.parser.CsvBytesParser;
import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.column.StringColumn;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.exception.CommonErrorCode;
import com.bytedance.bitsail.component.format.api.RowBuilder;
import com.bytedance.bitsail.flink.core.parser.BytesParser;
import com.bytedance.bitsail.parser.error.ParserErrorCode;

import lombok.NonNull;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

import java.util.stream.IntStream;

import static com.bytedance.bitsail.common.option.ReaderOptions.BaseReaderOptions.CONTENT_TYPE;

public class TextRowBuilder<T> implements RowBuilder<T> {

  private ContentType contentType;
  private BytesParser bytesParser;

  public TextRowBuilder(BitSailConfiguration inputSliceConfig) throws Exception {
    this.contentType = ContentType.valueOf(inputSliceConfig.getNecessaryOption(CONTENT_TYPE, ParserErrorCode.REQUIRED_VALUE).toUpperCase());

    if (contentType == ContentType.PROTOBUF || contentType == ContentType.JSON ||
        contentType == ContentType.CSV || contentType == ContentType.BINARY) {
      this.bytesParser = BytesParseFactory.initBytesParser(inputSliceConfig);
    }
  }

  @Override
  public void build(T value, Row reuse, RowTypeInfo rowTypeInfo) throws BitSailException {
    build(value, reuse, rowTypeInfo, IntStream.range(0, reuse.getArity()).toArray());
  }

  @Override
  public void build(T value, Row reuse, RowTypeInfo rowTypeInfo, int[] fieldIndexes) throws BitSailException {
    switch (contentType) {
      case JSON:
      case PROTOBUF:
      case BINARY:
        buildRowWithParser(value, reuse, rowTypeInfo, bytesParser);
        break;
      case CSV:
        buildRowWithCsvParser(value, reuse, rowTypeInfo, bytesParser, fieldIndexes);
        break;
      case PLAIN:
        buildPlainTextRow(value.toString(), reuse, rowTypeInfo);
        break;
      default:
        throw BitSailException.asBitSailException(CommonErrorCode.UNSUPPORTED_ENCODING, contentType + " not supported");
    }
  }

  /**
   * text file is a json/protobuf file which needs a parser to parse
   */
  private void buildRowWithParser(T value, Row reuse, RowTypeInfo rowTypeInfo, @NonNull BytesParser bytesParser) throws BitSailException {
    try {
      bytesParser.parse(reuse, value, rowTypeInfo);
    } catch (Exception e) {
      throw BitSailException.asBitSailException(ParserErrorCode.ILLEGAL_TEXT, "value: " + value.toString(), e);
    }
  }

  /**
   * text file is a csv file which needs a parser to parse
   */
  private void buildRowWithCsvParser(T value, Row reuse, RowTypeInfo rowTypeInfo, @NonNull BytesParser bytesParser,
                                     int[] fieldIndexes
  ) throws BitSailException {
    try {
      ((CsvBytesParser) bytesParser).parse(reuse, value, StringUtils.EMPTY, rowTypeInfo, fieldIndexes);
    } catch (Exception e) {
      throw BitSailException.asBitSailException(ParserErrorCode.ILLEGAL_TEXT, "value: " + value.toString(), e);
    }
  }

  /**
   * text file is a plain-text file which does not need a parser to parse
   */
  private void buildPlainTextRow(String value, Row reuse, RowTypeInfo rowTypeInfo) throws BitSailException {
    try {
      reuse.setField(0, new StringColumn(value));
    } catch (Exception e) {
      throw BitSailException.asBitSailException(ParserErrorCode.ILLEGAL_TEXT, "value: " + value, e);
    }
  }
}
