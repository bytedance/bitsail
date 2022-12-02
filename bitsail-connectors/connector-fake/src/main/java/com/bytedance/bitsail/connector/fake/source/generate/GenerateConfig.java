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

package com.bytedance.bitsail.connector.fake.source.generate;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

import java.sql.Timestamp;
import java.time.ZoneId;
import java.util.concurrent.atomic.AtomicLong;

@Builder
@Getter
@AllArgsConstructor
public class GenerateConfig {

  private Integer taskId;
  private final transient AtomicLong rowId;
  private final long upper;
  private final long lower;
  private final transient Timestamp fromTimestamp;
  private final transient Timestamp toTimestamp;
  private final ZoneId zoneId = ZoneId.systemDefault();

}
