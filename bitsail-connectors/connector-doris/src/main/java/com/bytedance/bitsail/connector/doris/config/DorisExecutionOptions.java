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

package com.bytedance.bitsail.connector.doris.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.io.Serializable;
import java.util.Properties;

/**
 * JDBC sink batch options.
 */
@AllArgsConstructor
@Builder
@Data
public class DorisExecutionOptions implements Serializable {

  private static final long serialVersionUID = 1L;

  private final int flushIntervalMs;
  private final int maxRetries;
  private final int bufferSize;
  private final int bufferCount;
  private final int recordCount;
  private final int recordSize;
  private final String labelPrefix;
  private final boolean isBatch;
  private final boolean enable2PC;
  private final int checkInterval;
  private final int requestConnectTimeoutMs;
  private final int requestReadTimeoutMs;
  private final int requestRetries;

  /**
   * Properties for the StreamLoad.
   */
  private final Properties streamLoadProp;

  private final Boolean enableDelete;

  private final WRITE_MODE writerMode;

  public enum WRITE_MODE {
    STREAMING_UPSERT,
    BATCH_REPLACE,
    BATCH_UPSERT
  }
}

