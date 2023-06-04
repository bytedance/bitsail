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

import com.bytedance.bitsail.common.annotation.Essential;
import com.bytedance.bitsail.common.option.ConfigOption;
import com.bytedance.bitsail.common.option.ReaderOptions;
import com.bytedance.bitsail.common.option.WriterOptions;

import com.alibaba.fastjson.TypeReference;

import java.util.Map;

import static com.bytedance.bitsail.common.option.ConfigOptions.key;

public interface KafkaOptions extends WriterOptions.BaseWriterOptions, ReaderOptions.BaseReaderOptions {
  ConfigOption<String> BOOTSTRAP_SERVERS =
      key("bootstrap_servers")
          .noDefaultValue(String.class)
          .withAlias("kafka_servers");

  @Essential
  ConfigOption<String> TOPIC_NAME =
      key("topic_name")
          .noDefaultValue(String.class);

  ConfigOption<String> KEY_FIELDS =
      key("key_fields")
          .noDefaultValue(String.class);

  ConfigOption<String> PARTITION_FIELD =
      key("partition_field")
          .noDefaultValue(String.class);

  ConfigOption<Boolean> LOG_FAILURES_ONLY =
      key("log_failures_only")
          .defaultValue(false);

  ConfigOption<Integer> RETRIES =
      key("retries")
          .defaultValue(10);

  /**
   * retry.backoff.ms
   */
  ConfigOption<Long> RETRY_BACKOFF_MS =
      key("retry_backoff_ms")
          .defaultValue(1000L);

  /**
   * linger.ms
   */
  ConfigOption<Long> LINGER_MS =
      key("linger_ms")
          .defaultValue(5000L);

  ConfigOption<String> FORMAT_TYPE =
      key("format_type")
          .defaultValue("json")
          .withAlias("content_type");

  ConfigOption<Map<String, String>> PROPERTIES =
      key("format_type")
          .onlyReference(new TypeReference<Map<String, String>>() {
          });
}