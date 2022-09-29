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

import com.bytedance.bitsail.common.ddl.typeinfo.ListTypeInfo;
import com.bytedance.bitsail.common.ddl.typeinfo.MapTypeInfo;
import com.bytedance.bitsail.common.ddl.typeinfo.PrimitiveTypes;
import com.bytedance.bitsail.common.ddl.typeinfo.TypeInfo;

import com.esotericsoftware.yamlbeans.YamlReader;
import com.google.common.collect.Maps;
import lombok.Getter;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.URL;
import java.text.MessageFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Created 2022/5/6
 */
public class TypeConverterReader implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(TypeConverterReader.class);
  private static final String ENGINE_CONVERTER_TEMPLATE_NAME = "{0}-type-converter.yaml";

  private static final String ENGINE_TO_CUSTOM_KEY = "engine.type.to.bitsail.type.converter";
  private static final String CUSTOM_TO_ENGINE_KEY = "bitsail.type.to.engine.type.converter";

  private final String converterFileName;

  @Getter
  protected Map<String, TypeInfo<?>> toTypeInformation = Maps.newHashMap();
  @Getter
  protected Map<TypeInfo<?>, String> fromTypeInformation = Maps.newHashMap();

  public TypeConverterReader(String engine) {
    LOG.info("Type converter reader read engine: {}.", engine);

    converterFileName = MessageFormat.format(ENGINE_CONVERTER_TEMPLATE_NAME, engine);
    try {
      read();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static TypeInfo<?> parseCustomTypeInfo(String customType) {
    if (PrimitiveTypes.isPrimitiveType(customType)) {
      return PrimitiveTypes.TYPE_STORE.get(customType);
    }

    if (PrimitiveTypes.isArrayType(customType)) {
      String elementType = PrimitiveTypes.getArrayElementType(customType);
      return new ListTypeInfo<>(parseCustomTypeInfo(elementType));
    }

    if (PrimitiveTypes.isMapType(customType)) {
      String[] mapKeyValueType = PrimitiveTypes.getMapKeyValueType(customType);
      return new MapTypeInfo<>(parseCustomTypeInfo(mapKeyValueType[0]), parseCustomTypeInfo(mapKeyValueType[1]));
    }

    throw new IllegalArgumentException(String.format("Custom type name %s parse failed.", customType));
  }

  @SuppressWarnings("unchecked")
  private void readerOption(Map<?, ?> converterConf,
                            String key,
                            Map<String, String> typeMapping) {
    if (converterConf.containsKey(key)) {
      List<Map<String, String>> typeMappings = (List<Map<String, String>>) converterConf.get(key);
      for (Map<String, String> mapping : typeMappings) {
        String sourceType = mapping.get("source.type");
        String targetType = mapping.get("target.type");
        if (StringUtils.isEmpty(sourceType) || StringUtils.isEmpty(targetType)) {
          throw new IllegalArgumentException(String.format("Source type: %s, target type: %s are not valid.",
              sourceType, targetType));
        }
        typeMapping.put(sourceType, targetType);
      }
    } else {
      LOG.warn("Converter conf can't find key: {} in converter file {}.", key, converterFileName);
    }
  }

  private void read() throws IOException {
    URL resource = TypeConverterReader.class.getResource("/" + converterFileName);
    if (Objects.isNull(resource)) {
      throw new IllegalArgumentException(String.format("Resource for the column converter %s not found in classpath.", converterFileName));
    }
    YamlReader yamlReader = new YamlReader(
        new InputStreamReader(
            resource.openStream()
        ));

    Map<?, ?> converterConf = yamlReader.read(Map.class);
    Map<String, String> tmpToTypeInformation = Maps.newHashMap();
    Map<String, String> tmpFromTypeInformation = Maps.newHashMap();
    readerOption(converterConf, ENGINE_TO_CUSTOM_KEY, tmpToTypeInformation);
    readerOption(converterConf, CUSTOM_TO_ENGINE_KEY, tmpFromTypeInformation);

    handleEngineTypeToCustom(tmpToTypeInformation);
    handleCustomToEngineType(tmpFromTypeInformation);
  }

  protected void handleEngineTypeToCustom(Map<String, String> tmpToTypeInformation) {
    for (Map.Entry<String, String> entry : tmpToTypeInformation.entrySet()) {

      TypeInfo<?> customTypeInfo = parseCustomTypeInfo(entry.getValue());

      if (Objects.isNull(customTypeInfo)) {
        throw new UnsupportedOperationException(String
            .format("Engine type %s not support transform to custom type.", entry.getValue()));
      }

      toTypeInformation.put(entry.getKey(), customTypeInfo);
    }
  }

  protected void handleCustomToEngineType(Map<String, String> tmpFromTypeInformation) {
    for (Map.Entry<String, String> entry : tmpFromTypeInformation.entrySet()) {
      if (!PrimitiveTypes.TYPE_STORE.containsKey(entry.getKey())) {
        throw new IllegalArgumentException(String.format("From Custom type %s is invalid.", entry.getKey()));
      }
      fromTypeInformation.put(PrimitiveTypes.TYPE_STORE.get(entry.getKey()), entry.getValue());
    }
  }
}
