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

package com.bytedance.bitsail.connector.selectdb.copyinto;

import com.bytedance.bitsail.connector.selectdb.config.SelectdbExecutionOptions;
import com.bytedance.bitsail.connector.selectdb.config.SelectdbOptions;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.StringJoiner;

public class CopySQLBuilder {
  private static final String COPY_SYNC = "copy.async";
  private static final String COPY_DELETE = "copy.use_delete_sign";
  private final SelectdbOptions selectdbOptions;
  private final SelectdbExecutionOptions executionOptions;
  private final List<String> fileList;
  private Properties properties;

  public CopySQLBuilder(SelectdbOptions selectdbOptions, SelectdbExecutionOptions executionOptions, List<String> fileList) {
    this.selectdbOptions = selectdbOptions;
    this.executionOptions = executionOptions;
    this.fileList = fileList;
    this.properties = executionOptions.getStreamLoadProp();
  }

  public String buildCopySQL() {
    StringBuilder sb = new StringBuilder();
    sb.append("COPY INTO ")
        .append(selectdbOptions.getTableIdentifier())
        .append(" FROM @~('{").append(String.join(",", fileList)).append("}') ")
        .append("PROPERTIES (");

    //copy into must be sync
    properties.put(COPY_SYNC, false);
    getFormatType(properties);
    if (executionOptions.getEnableDelete()) {
      properties.put(COPY_DELETE, true);
    }

    StringJoiner props = new StringJoiner(",");
    for (Map.Entry<Object, Object> entry : properties.entrySet()) {
      String key = String.valueOf(entry.getKey());
      String value = String.valueOf(entry.getValue());
      String prop = String.format("'%s'='%s'", key, value);
      props.add(prop);
    }
    sb.append(props).append(")");
    return sb.toString();
  }

  private void getFormatType(Properties properties) {
    SelectdbOptions.LOAD_CONTENT_TYPE loadDataFormat = selectdbOptions.getLoadDataFormat();
    if (loadDataFormat.equals(SelectdbOptions.LOAD_CONTENT_TYPE.JSON)) {
      properties.put("file.type", "json");
      properties.put("file.strip_outer_array", "false");
    } else if (loadDataFormat.equals(SelectdbOptions.LOAD_CONTENT_TYPE.CSV)) {
      properties.put("file.type", "csv");
      properties.put("file.line_delimiter", selectdbOptions.getLineDelimiter());
      properties.put("file.column_separator", selectdbOptions.getFieldDelimiter());
    } else {
      throw new IllegalArgumentException("This format is not supported, only supports json and csv, format=" + loadDataFormat);
    }
  }
}
