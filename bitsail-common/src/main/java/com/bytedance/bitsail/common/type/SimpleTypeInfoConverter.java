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

/**
 * Created 2022/5/11
 */
public class SimpleTypeInfoConverter extends BaseEngineTypeInfoConverter {

  public SimpleTypeInfoConverter(String engineName) {
    super(engineName);
  }

  @Override
  public TypeInfo<?> toTypeInfo(String engineType) {
    return reader.getToTypeInformation().get(engineType);
  }

  @Override
  public String fromTypeInfo(TypeInfo<?> typeInfo) {
    return fromTypeInfo(typeInfo, false);
  }

  @Override
  public String fromTypeInfo(TypeInfo<?> typeInfo, boolean nullable) {
    return reader.getFromTypeInformation().get(typeInfo);
  }
}
