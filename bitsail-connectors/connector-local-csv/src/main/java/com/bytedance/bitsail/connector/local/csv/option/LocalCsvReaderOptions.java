
package com.bytedance.bitsail.connector.local.csv.option;

import com.bytedance.bitsail.common.option.ConfigOption;
import com.bytedance.bitsail.component.format.csv.option.CsvReaderOptions;

import static com.bytedance.bitsail.common.option.ConfigOptions.key;
import static com.bytedance.bitsail.common.option.ReaderOptions.READER_PREFIX;

public interface LocalCsvReaderOptions extends CsvReaderOptions {
  ConfigOption<String> FILE_PATH =
      key(READER_PREFIX + "file_path")
      .defaultValue("");
}
