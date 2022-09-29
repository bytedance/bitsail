package com.bytedance.bitsail.connector.legacy.hudi.common;

import com.bytedance.bitsail.common.option.ConfigOption;

import static com.bytedance.bitsail.common.option.ConfigOptions.key;
import static com.bytedance.bitsail.common.option.WriterOptions.WRITER_PREFIX;

public interface SchemaOptions {
  ConfigOption<String> SOURCE_SCHEMA =
      key(WRITER_PREFIX + "source_schema")
          .noDefaultValue(String.class);

  ConfigOption<String> SINK_SCHEMA =
      key(WRITER_PREFIX + "sink_schema")
          .noDefaultValue(String.class);
}
