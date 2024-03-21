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

package com.bytedance.bitsail.common.type.filemapping;

import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.exception.CommonErrorCode;
import com.bytedance.bitsail.common.type.BitSailTypeParser;
import com.bytedance.bitsail.common.type.TypeInfoConverter;
import com.bytedance.bitsail.common.typeinfo.ListTypeInfo;
import com.bytedance.bitsail.common.typeinfo.MapTypeInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfo;
import com.bytedance.bitsail.common.typeinfo.Types;

import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

import java.util.Objects;

public class FileMappingTypeInfoConverter implements TypeInfoConverter {
  @Getter
  protected FileMappingTypeInfoReader reader;

  protected String engineName;

  public FileMappingTypeInfoConverter(String engineName) {
    this(engineName, new FileMappingTypeInfoReader(engineName));
  }

  public FileMappingTypeInfoConverter(String engineName, FileMappingTypeInfoReader reader) {
    this.engineName = engineName;
    this.reader = reader;
  }

  protected static String getBaseName(String typeName) {
    int idx = typeName.indexOf('(');
    if (idx == -1) {
      return typeName;
    } else {
      return typeName.substring(0, idx);
    }
  }

  protected static String trim(String typeName) {
    return StringUtils.replace(StringUtils.trim(typeName), " ", "");
  }

  @Override
  public TypeInfo<?> fromTypeString(String typeString) {
    TypeInfo<?> typeInfo = reader.getToTypeInformation().get(typeString);

    if (!Objects.isNull(typeInfo)) {
      return typeInfo;
    }

    String engineMapStr = reader.customToEngineTypeStringMap.get(Types.MAP.name().toLowerCase());
    String engineListStr = reader.customToEngineTypeStringMap.get(Types.LIST.name().toLowerCase());
    if (StringUtils.isNotBlank(engineMapStr) && typeString.contains(engineMapStr)
        || StringUtils.isNotBlank(engineListStr) && typeString.contains(engineListStr)) {
      String customTypeString = this.toCustomTypeString(typeString);
      return BitSailTypeParser.fromTypeString(customTypeString);
    }

    throw BitSailException.asBitSailException(CommonErrorCode.INTERNAL_ERROR,
      String.format("Not support type string %s.", typeString));
  }

  @Override
  public String fromTypeInfo(TypeInfo<?> typeInfo) {
    String typeStr = reader.getFromTypeInformation().get(typeInfo);

    if (StringUtils.isNotBlank(typeStr)) {
      return typeStr;
    }

    if (typeInfo instanceof ListTypeInfo || typeInfo instanceof MapTypeInfo) {
      String customTypeString = BitSailTypeParser.fromTypeInfo(typeInfo);
      return this.toEngineTypeString(customTypeString);
    }

    throw BitSailException.asBitSailException(CommonErrorCode.INTERNAL_ERROR,
      String.format("Not support typeInfo %s.", typeInfo));
  }

  private String toEngineTypeString(String customTypeString) {
    customTypeString = customTypeString.toLowerCase();
    for (String key : reader.customToEngineTypeStringMap.keySet()) {
      customTypeString = customTypeString.replace(key, reader.customToEngineTypeStringMap.get(key));
    }
    return customTypeString;
  }

  private String toCustomTypeString(String engineTypeString) {
    for (String key : reader.engineToCustomTypeStringMap.keySet()) {
      engineTypeString = engineTypeString.replace(key, reader.engineToCustomTypeStringMap.get(key));
    }
    return engineTypeString.toLowerCase();
  }
}
