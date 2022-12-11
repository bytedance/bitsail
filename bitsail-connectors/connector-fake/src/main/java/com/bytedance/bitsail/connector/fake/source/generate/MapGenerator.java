package com.bytedance.bitsail.connector.fake.source.generate;

import com.google.common.collect.Maps;

import java.util.Map;

public class MapGenerator implements ColumnDataGenerator {
  private final ColumnDataGenerator keyGenerator;
  private final ColumnDataGenerator valueGenerator;

  public MapGenerator(ColumnDataGenerator keyGenerator, ColumnDataGenerator valueGenerator) {
    this.keyGenerator = keyGenerator;
    this.valueGenerator = valueGenerator;
  }

  @Override
  public Object generate(GenerateConfig generateConfig) {
    Map<Object, Object> mapRawValue = Maps.newHashMap();
    mapRawValue.put(keyGenerator.generate(generateConfig), valueGenerator.generate(generateConfig));
    return mapRawValue;
  }
}
