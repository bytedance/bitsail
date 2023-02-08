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

package com.bytedance.bitsail.connector.legacy.redis.core;

import lombok.Getter;

public enum TtlType {

  DAY(60 * 60 * 24),

  HOUR(60 * 60),

  MINUTE(60),

  SECOND(1);

  @Getter
  int containSeconds;

  TtlType(int containSeconds) {
    this.containSeconds = containSeconds;
  }
}
