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

package com.bytedance.bitsail.connector.kafka.option;

import com.bytedance.bitsail.common.option.ConfigOption;
import com.bytedance.bitsail.common.option.ConfigOptions;
import com.bytedance.bitsail.common.option.ReaderOptions;

import com.alibaba.fastjson.TypeReference;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.bytedance.bitsail.common.option.ReaderOptions.READER_PREFIX;
import static com.bytedance.bitsail.connector.kafka.constants.KafkaConstants.CONSUMER_OFFSET_LATEST_KEY;
import static com.bytedance.bitsail.connector.kafka.constants.KafkaConstants.JSON_FORMAT;

public interface KafkaSourceOptions extends ReaderOptions.BaseReaderOptions {

  ConfigOption<String> BOOTSTRAP_SERVERS =
      ConfigOptions.key(READER_PREFIX + "bootstrap_servers")
          .noDefaultValue(String.class);

  ConfigOption<String> TOPIC =
      ConfigOptions.key(READER_PREFIX + "topic")
          .noDefaultValue(String.class);

  ConfigOption<String> CONSUMER_GROUP =
      ConfigOptions.key(READER_PREFIX + "consumer_group")
          .noDefaultValue(String.class);

  ConfigOption<Boolean> COMMIT_IN_CHECKPOINT =
      ConfigOptions.key(READER_PREFIX + "commit_in_checkpoint")
          .defaultValue(false);

  ConfigOption<Long> DISCOVERY_INTERNAL =
      ConfigOptions.key(READER_PREFIX + "discovery_internal_ms")
          .defaultValue(TimeUnit.MINUTES.toMillis(5L));

  ConfigOption<String> STARTUP_MODE =
      ConfigOptions.key(READER_PREFIX + "startup_mode")
          .defaultValue(CONSUMER_OFFSET_LATEST_KEY);

  ConfigOption<String> FORMAT_TYPE =
      ConfigOptions.key(READER_PREFIX + "format_type")
          .defaultValue(JSON_FORMAT);

  ConfigOption<Long> STARTUP_MODE_TIMESTAMP =
      ConfigOptions.key(READER_PREFIX + "startup_mode_timestamp")
          .noDefaultValue(Long.class);

  ConfigOption<String> CLIENT_ID_PREFIX =
      ConfigOptions.key(READER_PREFIX + "client_id_prefix")
          .noDefaultValue(String.class);

  ConfigOption<Map<String, String>> PROPERTIES =
      ConfigOptions.key(READER_PREFIX + "properties")
          .onlyReference(new TypeReference<Map<String, String>>() {
          });

  ConfigOption<Integer> POLL_BATCH_SIZE =
      ConfigOptions.key(READER_PREFIX + "poll_batch_size")
          .defaultValue(2048);

  ConfigOption<Long> POLL_TIMEOUT =
      ConfigOptions.key(READER_PREFIX + "poll_timeout")
          .defaultValue(TimeUnit.MINUTES.toMillis(1L));
}
