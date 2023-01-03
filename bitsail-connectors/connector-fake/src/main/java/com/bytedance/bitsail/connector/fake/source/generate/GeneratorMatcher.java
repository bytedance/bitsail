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

package com.bytedance.bitsail.connector.fake.source.generate;

import com.bytedance.bitsail.common.typeinfo.*;

import org.apache.commons.collections.CollectionUtils;

public class GeneratorMatcher {

  public static ColumnDataGenerator match(TypeInfo<?> typeInfo, GenerateConfig generateConfig) {
    if (typeInfo instanceof ListTypeInfo) {
      return new ListGenerator(match(typeInfo, generateConfig));
    }

    if (typeInfo instanceof MapTypeInfo) {
      return new MapGenerator(match(typeInfo, generateConfig), match(typeInfo, generateConfig));
    }

    // Data generator by snowFlake
    if (CollectionUtils.isNotEmpty(typeInfo.getTypeProperties()) && typeInfo.getTypeProperties().contains(TypeProperty.UNIQUE)) {
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
}
