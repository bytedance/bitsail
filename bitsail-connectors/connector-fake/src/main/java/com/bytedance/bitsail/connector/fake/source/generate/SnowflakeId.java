package com.bytedance.bitsail.connector.fake.source.generate;

import cn.ipokerface.snowflake.SnowflakeIdGenerator;

public class SnowflakeId implements ColumnDataGenerator {
  private final SnowflakeIdGenerator snowflakeIdGenerator;

  public SnowflakeId(long taskId) {
    this.snowflakeIdGenerator = new SnowflakeIdGenerator(taskId, taskId);
  }

  @Override
  public Object generate(GenerateConfig generateConfig) {
    return snowflakeIdGenerator.nextId();
  }
}
