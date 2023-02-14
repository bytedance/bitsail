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

package com.bytedance.bitsail.test.e2e.datasource.util;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.model.ColumnInfo;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.common.type.BitSailTypeInfoConverter;
import com.bytedance.bitsail.common.type.TypeInfoConverter;
import com.bytedance.bitsail.common.typeinfo.TypeInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfoUtils;
import com.bytedance.bitsail.connector.fake.option.FakeReaderOptions;
import com.bytedance.bitsail.connector.fake.source.FakeRowGenerator;

import lombok.NonNull;

import java.util.List;

public class RowGenerator {

  /**
   * Type infos for generating random fields.
   */
  private final TypeInfo<?>[] typeInfos;

  /**
   * Fake generator for produce data.
   */
  FakeRowGenerator fakeRowGenerator;

  public RowGenerator(List<ColumnInfo> columnInfos) {
    this(new BitSailTypeInfoConverter(), columnInfos);
  }

  public RowGenerator(TypeInfoConverter typeInfoConverter, List<ColumnInfo> columnInfos) {
    this(TypeInfoUtils.getTypeInfos(typeInfoConverter, columnInfos));
  }

  public RowGenerator(TypeInfo<?>[] typeInfos) {
    this.typeInfos = typeInfos;
  }

  public void init() {
    BitSailConfiguration conf = BitSailConfiguration.newDefault();
    init(conf);
  }

  public void init(@NonNull BitSailConfiguration conf) {
    conf.setIfAbsent(FakeReaderOptions.NULL_PERCENTAGE, 0);
    this.fakeRowGenerator = new FakeRowGenerator(conf, 0);
  }

  public Row next() {
    return fakeRowGenerator.fakeOneRecord(typeInfos);
  }
}
