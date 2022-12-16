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

package com.bytedance.bitsail.connector.hadoop.option;

import com.bytedance.bitsail.common.annotation.Essential;
import com.bytedance.bitsail.common.option.ConfigOption;
import com.bytedance.bitsail.common.option.ReaderOptions;

import static com.bytedance.bitsail.common.option.ConfigOptions.key;
import static com.bytedance.bitsail.common.option.ReaderOptions.READER_PREFIX;

public interface HadoopReaderOptions extends ReaderOptions.BaseReaderOptions {
  @Essential
  ConfigOption<String> DEFAULT_FS =
      key(READER_PREFIX + "defaultFS")
          .noDefaultValue(String.class);
  @Essential
  ConfigOption<String> PATH_LIST =
      key(READER_PREFIX + "path_list")
          .noDefaultValue(String.class);

  @Essential
  ConfigOption<String> CONTENT_TYPE =
      key(READER_PREFIX + "content_type")
          .noDefaultValue(String.class);

  ConfigOption<Integer> READER_PARALLELISM_NUM =
      key(READER_PREFIX + "reader_parallelism_num")
          .noDefaultValue(Integer.class);

  ConfigOption<Integer> DEFAULT_HADOOP_PARALLELISM_THRESHOLD =
      key(READER_PREFIX + "default_hadoop_parallelism_threshold")
          .defaultValue(2);
}