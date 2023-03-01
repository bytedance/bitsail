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

package com.bytedance.bitsail.connector.fake.source.generate;

import com.bytedance.bitsail.common.typeinfo.ListTypeInfo;
import com.bytedance.bitsail.common.typeinfo.MapTypeInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfos;
import com.bytedance.bitsail.common.typeinfo.TypeProperty;

public class GeneratorMatcher {
  public static ColumnDataGenerator match(TypeInfo<?> typeInfo, GenerateConfig generateConfig) {
    if (typeInfo instanceof ListTypeInfo) {
      return new ListGenerator(match(((ListTypeInfo<?>) typeInfo).getElementTypeInfo(), generateConfig));
    }

    if (typeInfo instanceof MapTypeInfo) {
      return new MapGenerator(match(((MapTypeInfo<?, ?>) typeInfo).getKeyTypeInfo(), generateConfig),
          match(((MapTypeInfo<?, ?>) typeInfo).getValueTypeInfo(), generateConfig));
    }

    // Data generator by snowFlake
    if (supportedUnique(typeInfo)) {
      if (TypeInfos.LONG_TYPE_INFO.getTypeClass() == typeInfo.getTypeClass()) {
        return new SnowflakeId(generateConfig.getTaskId());
      }
      // Data Generator by AutoIncrementData
      return new AutoIncrementData(typeInfo);
    }
    // Data Generator by FakeData
    if (FakerData.supported(typeInfo)) {
      return new FakerData(typeInfo);
    }
    throw new RuntimeException("Unsupported type " + typeInfo);
  }

  private static boolean supportedUnique(TypeInfo<?> typeInfo) {
    try {
      return typeInfo.getTypeProperties().contains(TypeProperty.UNIQUE);
    } catch (Exception ex) {
      // some type not supported typeProperties method will throw exception
      return false;
    }
  }
}
