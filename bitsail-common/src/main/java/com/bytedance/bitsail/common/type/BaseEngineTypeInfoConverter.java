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

package com.bytedance.bitsail.common.type;

import com.bytedance.bitsail.common.ddl.typeinfo.TypeInfo;

import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

public abstract class BaseEngineTypeInfoConverter {

  @Getter
  protected TypeConverterReader reader;

  protected String engineName;

  public BaseEngineTypeInfoConverter(String engineName) {
    this(engineName, new TypeConverterReader(engineName));
  }

  public BaseEngineTypeInfoConverter(String engineName, TypeConverterReader reader) {
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

  public TypeInfo<?> toTypeInfo(String engineType) {
    throw new UnsupportedOperationException("Unsupported the parse type info from engine type.");
  }

  public String fromTypeInfo(TypeInfo<?> typeInfo) {
    return fromTypeInfo(typeInfo, false);
  }

  public String fromTypeInfo(TypeInfo<?> typeInfo, boolean nullable) {
    throw new UnsupportedOperationException(String.format("Unsupported the get type info: %s from custom type.", typeInfo));
  }
}
