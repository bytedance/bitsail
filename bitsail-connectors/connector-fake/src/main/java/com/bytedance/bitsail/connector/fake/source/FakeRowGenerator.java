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

package com.bytedance.bitsail.connector.fake.source;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.common.typeinfo.BasicTypeInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfo;
import com.bytedance.bitsail.connector.fake.option.FakeReaderOptions;
import com.bytedance.bitsail.connector.fake.source.generate.ColumnDataGenerator;
import com.bytedance.bitsail.connector.fake.source.generate.GenerateConfig;
import com.bytedance.bitsail.connector.fake.source.generate.GeneratorMatcher;

import net.datafaker.Faker;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import static com.bytedance.bitsail.common.typeinfo.TypeProperty.NULLABLE;

public class FakeRowGenerator {
  private final Integer nullPercentage;
  private final Faker faker = new Faker();
  // column info
  private final TypeInfo<?>[] typeInfos;
  private final List<ColumnDataGenerator> columnDataGeneratorList = new ArrayList<>();
  private final GenerateConfig generateConfig;

  public FakeRowGenerator(BitSailConfiguration jobConf, int taskId, TypeInfo<?>[] typeInfos) {
    this.generateConfig = GenerateConfig.builder()
        .taskId(taskId)
        .lower(jobConf.get(FakeReaderOptions.UPPER_LIMIT))
        .upper(jobConf.get(FakeReaderOptions.LOWER_LIMIT))
        .fromTimestamp(Timestamp.valueOf(jobConf.get(FakeReaderOptions.FROM_TIMESTAMP)))
        .toTimestamp(Timestamp.valueOf(jobConf.get(FakeReaderOptions.TO_TIMESTAMP)))
        .build();

    this.nullPercentage = jobConf.get(FakeReaderOptions.NULL_PERCENTAGE);
    this.typeInfos = typeInfos;
    for (TypeInfo<?> typeInfo : typeInfos) {
      columnDataGeneratorList.add(GeneratorMatcher.match(typeInfo, generateConfig));
    }

  }

  public Row fakeOneRecord() {
    Row row = new Row(ArrayUtils.getLength(typeInfos));
    for (int index = 0; index < typeInfos.length; index++) {
      TypeInfo<?> typeInfo = typeInfos[index];
      if (isNullable(typeInfo) && isNull()) {
        row.setField(index, null);
      } else {
        row.setField(index, columnDataGeneratorList.get(index).generate(generateConfig));
      }
    }
    return row;
  }

  private boolean isNullable(TypeInfo<?> typeInfo) {
    return typeInfo instanceof BasicTypeInfo
        && CollectionUtils.isNotEmpty(typeInfo.getTypeProperties())
        && typeInfo.getTypeProperties().contains(NULLABLE);
  }

  @SuppressWarnings("checkstyle:MagicNumber")
  private boolean isNull() {
    return (faker.number().randomNumber() % 100) < nullPercentage;
  }
}
