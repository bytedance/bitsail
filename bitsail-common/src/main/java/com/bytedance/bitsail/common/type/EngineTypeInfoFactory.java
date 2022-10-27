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

import com.google.common.annotations.Beta;
import com.google.common.collect.Maps;

import java.util.Map;

/**
 * Created 2022/5/11
 * Only temporary use for old plugin format to acquire typeinfo, we will remove this class in future.
 */
@Beta
public class EngineTypeInfoFactory {

  private static volatile Map<String, BaseEngineTypeInfoConverter> CONVERTERS = Maps.newHashMap();

  public static synchronized BaseEngineTypeInfoConverter getEngineConverter(String engineName) {
    if (CONVERTERS.containsKey(engineName)) {
      return CONVERTERS.get(engineName);
    }
    BaseEngineTypeInfoConverter converter = null;
    switch (engineName) {
      case "hive":
        converter = new HiveTypeInfoConverter(engineName);
        break;
      case "mongodb":
        converter = new MongoTypeInfoConverter();
        break;
      default:
        converter = new SimpleTypeInfoConverter(engineName);
        break;
    }
    CONVERTERS.put(engineName, converter);
    return converter;
  }
}
