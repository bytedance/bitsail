/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bytedance.bitsail.connector.legacy.fake.source;

import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.column.BytesColumn;
import com.bytedance.bitsail.common.column.Column;
import com.bytedance.bitsail.common.column.DateColumn;
import com.bytedance.bitsail.common.column.DoubleColumn;
import com.bytedance.bitsail.common.column.LongColumn;
import com.bytedance.bitsail.common.column.StringColumn;
import com.bytedance.bitsail.common.model.ColumnInfo;
import com.bytedance.bitsail.common.option.ReaderOptions;
import com.bytedance.bitsail.connector.legacy.fake.option.FakeReaderOptions;
import com.bytedance.bitsail.flink.core.legacy.connector.InputFormatPlugin;
import com.bytedance.bitsail.flink.core.typeinfo.PrimitiveColumnTypeInfo;
import com.bytedance.bitsail.flink.core.typeutils.ColumnFlinkTypeInfoUtil;

import com.github.javafaker.Faker;
import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.shaded.guava18.com.google.common.util.concurrent.RateLimiter;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.function.Supplier;

public class FakeSource extends InputFormatPlugin<Row, InputSplit> implements ResultTypeQueryable<Row> {
  private static final Logger LOG = LoggerFactory.getLogger(FakeSource.class);

  private int totalCount;
  private int count;
  private int rate;
  private RowTypeInfo rowTypeInfo;
  private List<ColumnInfo> columnInfos;
  private int randomNullInt;
  private transient Faker faker;
  private transient Random random;
  private transient RateLimiter rateLimiter;
  private Map<String, Set<String>> uniqueFields;

  @Override
  public Row buildRow(Row reuse, String mandatoryEncoding) throws BitSailException {
    rateLimiter.acquire();
    count++;
    return createRow(reuse);
  }

  private Row createRow(Row reuse) {
    for (int index = 0; index < columnInfos.size(); index++) {
      String fieldName = columnInfos.get(index).getName();
      reuse.setField(index, createColumn(rowTypeInfo.getTypeAt(index), uniqueFields.get(fieldName)));
    }
    return reuse;
  }

  @SuppressWarnings("checkstyle:MagicNumber")
  private Column createColumn(TypeInformation<?> typeInformation, Set<String> existValues) {
    boolean isNull = randomNullInt > random.nextInt(10);
    if (PrimitiveColumnTypeInfo.LONG_COLUMN_TYPE_INFO.equals(typeInformation)) {
      long randomNumber = constructRandomValue(existValues, () -> faker.number().randomNumber());
      return isNull ? new LongColumn() : new LongColumn(randomNumber);
    } else if (PrimitiveColumnTypeInfo.STRING_COLUMN_TYPE_INFO.equals(typeInformation)) {
      String randomString = constructRandomValue(existValues, () -> faker.letterify("string_test_????"));
      return isNull ? new StringColumn() : new StringColumn(randomString);
    } else if (PrimitiveColumnTypeInfo.DOUBLE_COLUMN_TYPE_INFO.equals(typeInformation)) {
      double randomDouble = constructRandomValue(existValues, () -> faker.number().randomDouble(5, -1_000_000_000, 1_000_000_000));
      return isNull ? new DoubleColumn() : new DoubleColumn(randomDouble);
    } else if (PrimitiveColumnTypeInfo.BYTES_COLUMN_TYPE_INFO.equals(typeInformation)) {
      String randomString = constructRandomValue(existValues, () -> faker.numerify("test_#####"));
      return isNull ? new BytesColumn() : new BytesColumn(randomString.getBytes());
    } else if (PrimitiveColumnTypeInfo.DATE_COLUMN_TYPE_INFO.equals(typeInformation)) {
      Date randomDate = constructRandomValue(existValues, () -> faker.date().birthday(10, 30));
      return isNull ? new DateColumn() : new DateColumn(randomDate);
    }
    throw new RuntimeException("Unsupported type " + typeInformation);
  }

  @Override
  public boolean isSplitEnd() throws IOException {
    this.hasNext = count < totalCount;
    return !hasNext;
  }

  @Override
  public InputSplit[] createSplits(int minNumSplits) throws IOException {
    return new InputSplit[] {
        new FakeInputSplit()
    };
  }

  @SuppressWarnings("checkstyle:MagicNumber")
  @Override
  public void initPlugin() throws Exception {
    totalCount = inputSliceConfig.get(FakeReaderOptions.TOTAL_COUNT);
    rate = inputSliceConfig.get(FakeReaderOptions.RATE);
    randomNullInt = (int) Math.floor(inputSliceConfig.get(FakeReaderOptions.RANDOM_NULL_RATE) * 10);
    this.columnInfos = inputSliceConfig.get(ReaderOptions.BaseReaderOptions.COLUMNS);
    this.rowTypeInfo = ColumnFlinkTypeInfoUtil.getRowTypeInformation("bitsail", columnInfos);
    this.uniqueFields = initUniqueFieldsMapping(inputSliceConfig.get(FakeReaderOptions.UNIQUE_FIELDS));

    if (!uniqueFields.isEmpty() && totalCount > 1000) {
      LOG.warn("Unique fields is set and total count is larger than 1000, which may cause OOM problem.");
    }
  }

  @Override
  public String getType() {
    return "fake-source";
  }

  @Override
  public BaseStatistics getStatistics(BaseStatistics baseStatistics) throws IOException {
    return new BaseStatistics() {
      @Override
      public long getTotalInputSize() {
        return SIZE_UNKNOWN;
      }

      @Override
      public long getNumberOfRecords() {
        if (totalCount < 0) {
          return NUM_RECORDS_UNKNOWN;
        }
        return totalCount;
      }

      @Override
      public float getAverageRecordWidth() {
        return AVG_RECORD_BYTES_UNKNOWN;
      }
    };
  }

  @Override
  public InputSplitAssigner getInputSplitAssigner(InputSplit[] inputSplits) {
    return new DefaultInputSplitAssigner(inputSplits);
  }

  @Override
  public void open(InputSplit inputSplit) throws IOException {
    faker = new Faker(Locale.CHINA);
    random = new Random();
    rateLimiter = RateLimiter.create(rate);
    count = 0;
  }

  @Override
  public void close() throws IOException {

  }

  @Override
  public TypeInformation<Row> getProducedType() {
    return rowTypeInfo;
  }

  private static class FakeInputSplit implements InputSplit {

    @Override
    public int getSplitNumber() {
      return 0;
    }
  }

  @VisibleForTesting
  public static <T> T constructRandomValue(Set<String> existValues, Supplier<T> constructor) {
    T value;
    do {
      value = constructor.get();
    } while(ifNeedReconstruct(existValues, value));
    return value;
  }

  private static boolean ifNeedReconstruct(Set<String> existValues, Object value) {
    if (Objects.isNull(existValues)) {
      return false;
    }
    boolean exist = existValues.contains(value.toString());
    existValues.add(value.toString());
    return exist;
  }

  @VisibleForTesting
  public static Map<String, Set<String>> initUniqueFieldsMapping(String uniqueFieldsStr) {
    Map<String, Set<String>> uniqueFieldsMapping = new HashMap<>();
    if (StringUtils.isNotEmpty(uniqueFieldsStr) && StringUtils.isNotBlank(uniqueFieldsStr)) {
      Arrays.stream(uniqueFieldsStr.trim().split(",")).forEach(field -> uniqueFieldsMapping.put(field, new HashSet<>()));
    }
    return uniqueFieldsMapping;
  }
}
