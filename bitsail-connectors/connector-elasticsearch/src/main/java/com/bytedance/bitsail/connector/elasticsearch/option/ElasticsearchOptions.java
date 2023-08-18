/*
 *       Copyright 2022-2023 Bytedance Ltd. and/or its affiliates.
 *
 *       Licensed under the Apache License, Version 2.0 (the "License");
 *       you may not use this file except in compliance with the License.
 *       You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *       Unless required by applicable law or agreed to in writing, software
 *       distributed under the License is distributed on an "AS IS" BASIS,
 *       WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *       See the License for the specific language governing permissions and
 *       limitations under the License.
 */

package com.bytedance.bitsail.connector.elasticsearch.option;

import com.bytedance.bitsail.common.option.ConfigOption;

import java.io.Serializable;

import static com.bytedance.bitsail.common.option.ConfigOptions.key;

public interface ElasticsearchOptions extends Serializable {

  ConfigOption<String> HOSTS =
      key("hosts")
          .noDefaultValue(String.class)
          .withAlias("es_hosts");

  ConfigOption<String> USERNAME =
      key("username")
          .noDefaultValue(String.class)
          .withAlias("es_username");

  ConfigOption<String> PASSWORD =
      key("password")
          .noDefaultValue(String.class)
          .withAlias("es_password");

  ConfigOption<String> PATH_PREFIX =
      key("path_prefix")
          .noDefaultValue(String.class);

  ConfigOption<String> INDEX =
      key("index")
          .noDefaultValue(String.class)
          .withAlias("es_index");

  ConfigOption<String> ID_FIELD_DELIMITER =
      key("id_delimiter")
          .defaultValue("#");

  ConfigOption<String> ID_KEY_FIELDS =
      key("id_key_fields")
          .noDefaultValue(String.class)
          .withAlias("es_id_fields");

  ConfigOption<String> ROUTING_KEY_FIELD =
      key("routing_key")
          .noDefaultValue(String.class)
          .withAlias("es_shard_routing_fields");

  ConfigOption<String> INDEX_KEY_FIELD =
      key("index_key_field")
          .noDefaultValue(String.class)
          .withAlias("es_dynamic_index_field");


  ConfigOption<Integer> CONNECTION_REQUEST_TIMEOUT_MS =
      key("connection_request_timeout_ms")
          .defaultValue(10000);

  ConfigOption<Integer> CONNECTION_TIMEOUT_MS =
      key("connection_timeout_ms")
          .defaultValue(10000);

  ConfigOption<Integer> SOCKET_TIMEOUT_MS =
      key("socket_timeout_ms")
          .defaultValue(60000);

  @SuppressWarnings("checkstyle:MagicNumber")
  ConfigOption<Integer> BULK_MAX_BATCH_COUNT =
      key("bulk_max_batch_count")
          .defaultValue(300)
          .withAlias("bulk_flush_max_actions");

  @SuppressWarnings("checkstyle:MagicNumber")
  ConfigOption<Integer> BULK_MAX_BATCH_SIZE =
      key("bulk_max_batch_size")
          .defaultValue(10)
          .withAlias("bulk_flush_max_size_mb");

  @SuppressWarnings("checkstyle:MagicNumber")
  ConfigOption<Integer> BULK_MAX_RETRY_COUNT =
      key("bulk_max_retry_count")
          .defaultValue(5)
          .withAlias("bulk_backoff_max_retry_count");

  ConfigOption<Long> BULK_FLUSH_INTERVAL_MS =
      key("bulk_flush_interval_ms")
          .defaultValue(60 * 1000L);

  ConfigOption<Integer> BULK_BACKOFF_DELAY_MS =
      key("bulk_backoff_delay_ms")
          .defaultValue(100);
}

