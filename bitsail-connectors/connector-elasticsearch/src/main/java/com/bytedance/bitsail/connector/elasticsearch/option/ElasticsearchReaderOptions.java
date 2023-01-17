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

package com.bytedance.bitsail.connector.elasticsearch.option;

import com.bytedance.bitsail.common.option.ConfigOption;
import com.bytedance.bitsail.common.option.ReaderOptions;

import com.alibaba.fastjson.TypeReference;

import java.util.List;

import static com.bytedance.bitsail.common.option.ConfigOptions.key;

public interface ElasticsearchReaderOptions extends ReaderOptions.BaseReaderOptions {

  ConfigOption<List<String>> ES_HOSTS =
      key(ReaderOptions.READER_PREFIX + "es_hosts")
          .onlyReference(new TypeReference<List<String>>() {
          });

  ConfigOption<String> ES_INDEX =
      key(ReaderOptions.READER_PREFIX + "es_index")
          .noDefaultValue(String.class);

  ConfigOption<Integer> CONNECTION_REQUEST_TIMEOUT_MS =
      key(ReaderOptions.READER_PREFIX + "connection_request_timeout_ms")
          .defaultValue(10000);

  ConfigOption<Integer> CONNECTION_TIMEOUT_MS =
      key(ReaderOptions.READER_PREFIX + "connection_timeout_ms")
          .defaultValue(10000);

  ConfigOption<Integer> SOCKET_TIMEOUT_MS =
      key(ReaderOptions.READER_PREFIX + "socket_timeout_ms")
          .defaultValue(60000);

  ConfigOption<String> SCROLL_TIME =
      key(ReaderOptions.READER_PREFIX + "scroll_time")
          .defaultValue("1m");

  ConfigOption<Integer> SCROLL_SIZE =
      key(ReaderOptions.READER_PREFIX + "scroll_size")
          .defaultValue(100);

  ConfigOption<String> SPLIT_STRATEGY =
      key(ReaderOptions.READER_PREFIX + "split_strategy")
          .defaultValue("round_robin");
}
