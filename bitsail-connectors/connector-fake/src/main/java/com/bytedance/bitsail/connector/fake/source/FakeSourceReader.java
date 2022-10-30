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

import com.google.common.util.concurrent.RateLimiter;
import net.datafaker.Faker;
import org.apache.commons.lang3.ArrayUtils;

import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class FakeSourceReader extends SimpleSourceReaderBase<Row> {

  private BitSailConfiguration readerConfiguration;
  private TypeInfo<?>[] typeInfos;
  private List<ColumnInfo> columnInfos;

  private final transient Faker faker;
  private final transient int totalCount;
  private final transient RateLimiter fakeGenerateRate;
  private final transient AtomicLong counter;

  public FakeSourceReader(BitSailConfiguration readerConfiguration, Context context) {
    this.readerConfiguration = readerConfiguration;
    this.typeInfos = context.getTypeInfos();
    this.columnInfos = readerConfiguration.get(ReaderOptions.BaseReaderOptions.COLUMNS);
    this.totalCount = readerConfiguration.get(FakeReaderOptions.TOTAL_COUNT);
    this.fakeGenerateRate = RateLimiter.create(readerConfiguration.get(FakeReaderOptions.RATE));
    this.faker = new Faker();
    this.counter = new AtomicLong();
  }

  @Override
  public void pollNext(SourcePipeline<Row> pipeline) throws Exception {
    fakeGenerateRate.acquire();
    pipeline.output(fakeNextRecord());
  }

  @Override
  public boolean hasMoreElements() {
    return counter.incrementAndGet() <= totalCount;
  }

  private Row fakeNextRecord() {
    Row row = new Row(ArrayUtils.getLength(typeInfos));
    for (int index = 0; index < columnInfos.size(); index++) {
      row.setField(index, fakeRawValue(typeInfos[index]));
    }
    return row;
  }

  @SuppressWarnings("checkstyle:MagicNumber")
  private Object fakeRawValue(TypeInfo<?> typeInfo) {
    if (PrimitiveTypes.LONG.getTypeInfo().getTypeClass() == typeInfo.getTypeClass()) {
      return faker.number().randomNumber();

    } else if (PrimitiveTypes.STRING.getTypeInfo().getTypeClass() == typeInfo.getTypeClass()) {
      return faker.name().fullName();

    } else if (PrimitiveTypes.BOOLEAN.getTypeInfo().getTypeClass() == typeInfo.getTypeClass()) {
      return faker.bool().bool();

    } else if (PrimitiveTypes.DOUBLE.getTypeInfo().getTypeClass() == typeInfo.getTypeClass()) {
      return faker.number().randomDouble(5, -1_000_000_000, 1_000_000_000);

    } else if (PrimitiveTypes.BINARY.getTypeInfo().getTypeClass() == typeInfo.getTypeClass()) {
      return faker.name().fullName().getBytes();

    } else if (PrimitiveTypes.DATE_DATE.getTypeInfo().getTypeClass() == typeInfo.getTypeClass()) {
      return new java.sql.Date(faker.date().birthday(10, 99).getTime());

    } else if (PrimitiveTypes.DATE_TIME.getTypeInfo().getTypeClass() == typeInfo.getTypeClass()) {
      return new Time(faker.date().birthday(10, 99).getTime());

    } else if (PrimitiveTypes.DATE_DATE_TIME.getTypeInfo().getTypeClass() == typeInfo.getTypeClass()) {
      return new Timestamp(faker.date().birthday(10, 99).getTime());
    }
    throw new RuntimeException("Unsupported type " + typeInfo);
  }
}
