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

package com.bytedance.bitsail.connector.legacy.redis.constant;

public class RedisConstants {

  public static final int MAX_PARALLELISM_OUTPUT_REDIS = 5;

  public static final int DEFAULT_MAX_WAIT_TIME_IN_MILLS = 60000;
  public static final int DEFAULT_MAX_TOTAL_CONNECTIONS = 2;
  public static final int DEFAULT_MAX_IDLE_CONNECTIONS = 2;
  public static final int DEFAULT_MIN_IDLE_CONNECTIONS = 0;
}
