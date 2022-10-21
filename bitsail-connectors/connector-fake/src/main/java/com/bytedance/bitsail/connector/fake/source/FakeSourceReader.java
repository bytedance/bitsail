/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.bytedance.bitsail.connector.fake.source;

import com.bytedance.bitsail.base.connector.reader.v1.SourcePipeline;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.ddl.typeinfo.PrimitiveTypes;
import com.bytedance.bitsail.common.ddl.typeinfo.TypeInfo;
import com.bytedance.bitsail.common.model.ColumnInfo;
import com.bytedance.bitsail.common.option.ReaderOptions;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.connector.base.source.SimpleSourceReaderBase;
import com.bytedance.bitsail.connector.fake.option.FakeReaderOptions;

import net.datafaker.Faker;
import org.apache.commons.lang3.ArrayUtils;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class FakeSourceReader extends SimpleSourceReaderBase<Row> {

  private BitSailConfiguration readerConfiguration;
  private TypeInfo<?>[] typeInfos;
  private List<ColumnInfo> columnInfos;

  private final transient Faker faker;
  private final transient int totalCount;
  private final transient double fakeGenerateRate;
  private final transient double fakeNullableRate;
  private final transient AtomicLong counter;

  public FakeSourceReader(BitSailConfiguration readerConfiguration, Context context) {
    this.readerConfiguration = readerConfiguration;
    this.typeInfos = context.getTypeInfos();
    this.columnInfos = readerConfiguration.get(ReaderOptions.BaseReaderOptions.COLUMNS);
    this.totalCount = readerConfiguration.get(FakeReaderOptions.TOTAL_COUNT);
    this.fakeGenerateRate = readerConfiguration.get(FakeReaderOptions.RATE);
    this.fakeNullableRate = readerConfiguration.get(FakeReaderOptions.RANDOM_NULL_RATE);
    this.faker = new Faker();
    this.counter = new AtomicLong();
  }

  @Override
  public void pollNext(SourcePipeline<Row> pipeline) throws Exception {
    pipeline.output(createRow());
  }

  @Override
  public boolean hasMoreElements() {
    return counter.incrementAndGet() <= totalCount;
  }

  private Row createRow() {
    Row row = new Row(ArrayUtils.getLength(typeInfos));
    for (int index = 0; index < columnInfos.size(); index++) {
      row.setField(index, createObject(typeInfos[index]));
    }
    return row;
  }

  private Object createObject(TypeInfo<?> typeInfo) {
    if (PrimitiveTypes.LONG.getTypeInfo() == typeInfo) {
      return faker.number().randomNumber();
    }
    return null;
  }
}
