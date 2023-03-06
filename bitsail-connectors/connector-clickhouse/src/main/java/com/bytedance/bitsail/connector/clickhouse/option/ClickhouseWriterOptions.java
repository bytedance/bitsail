/*
 * Copyright 2022 Bytedance Ltd. and/or its affiliates.
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

package com.bytedance.bitsail.connector.clickhouse.option;

import com.bytedance.bitsail.common.annotation.Essential;
import com.bytedance.bitsail.common.option.ConfigOption;
import com.bytedance.bitsail.common.option.WriterOptions;

import com.alibaba.fastjson.TypeReference;

import java.util.Map;

import static com.bytedance.bitsail.common.option.ConfigOptions.key;
import static com.bytedance.bitsail.common.option.WriterOptions.WRITER_PREFIX;

public interface ClickhouseWriterOptions extends WriterOptions.BaseWriterOptions {
  /**
   * Standard format:
   * jdbc:(ch|clickhouse)[:protocol]://endpoint[,endpoint][/database][?parameters][#tags]<br/>
   *  - endpoint: [protocol://]host[:port][/database][?parameters][#tags]<br/>
   *  - protocol: (grpc|grpcs|http|https|tcp|tcps)
   */
  @Essential
  ConfigOption<String> JDBC_URL =
      key(WRITER_PREFIX + "jdbc_url")
          .noDefaultValue(String.class);

/*  ConfigOption<String> WRITE_MODE =
      key(WRITER_PREFIX + "write_mode")
      .noDefaultValue(String.class);

  ConfigOption<String> WRITER_PARALLELISM_NUM =
      key(WRITER_PREFIX + "writer_parallelism_num")
      .noDefaultValue(String.class);*/

  // Connection properties.
  ConfigOption<Map<String, String>> CUSTOMIZED_CONNECTION_PROPERTIES =
      key(WRITER_PREFIX + "customized_connection_properties")
      .onlyReference(new TypeReference<Map<String, String>>() {});

  ConfigOption<Integer> BATCH_SIZE =
      key(WRITER_PREFIX + "batch_size")
      .defaultValue(10);
}
