package com.bytedance.bitsail.connector.fake.source.generate;

import com.google.common.collect.Lists;

public class ListGenerator implements ColumnDataGenerator {

  private final ColumnDataGenerator elementGenerator;

  public ListGenerator(ColumnDataGenerator elementGenerator) {
    this.elementGenerator = elementGenerator;
  }

  @Override
  public Object generate(ColumnConfig columnConfig) {
    return Lists.newArrayList(elementGenerator.generate(columnConfig));
  }
}
