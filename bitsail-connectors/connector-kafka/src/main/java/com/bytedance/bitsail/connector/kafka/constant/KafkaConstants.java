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

package com.bytedance.bitsail.connector.kafka.constant;

public class KafkaConstants {

  public static String KAFKA_CONNECTOR_NAME = "kafka";

  public static final String CONSUMER_OFFSET_LATEST_KEY = "latest";

  public static final String CONSUMER_OFFSET_EARLIEST_KEY = "earliest";

  public static final String CONSUMER_OFFSET_TIMESTAMP_KEY = "timestamp";

  public static final Long CONSUMER_STOPPING_OFFSET = Long.MAX_VALUE;

  public static final String DEFAULT_CLIENT_ID = "-coordinator-admin-client-";

  public static final String JSON_FORMAT = "json";
}
