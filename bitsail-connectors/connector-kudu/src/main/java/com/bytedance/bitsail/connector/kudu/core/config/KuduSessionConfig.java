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

package com.bytedance.bitsail.connector.kudu.core.config;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.util.Preconditions;
import com.bytedance.bitsail.connector.kudu.option.KuduWriterOptions;

import lombok.Getter;
import org.apache.kudu.client.ExternalConsistencyMode;
import org.apache.kudu.client.SessionConfiguration.FlushMode;

import java.io.Serializable;

@Getter
public class KuduSessionConfig implements Serializable {

  private final FlushMode flushMode;
  private final ExternalConsistencyMode consistencyMode;

  private final Integer mutationBufferSize;
  private final Integer flushInterval;
  private final Long timeout;
  private final Boolean ignoreDuplicateRows;

  /**
   * Init kudu client configurations.
   * @param jobConf Job configuration.
   */
  public KuduSessionConfig(BitSailConfiguration jobConf) {
    this.flushMode = FlushMode.valueOf(jobConf.get(KuduWriterOptions.SESSION_FLUSH_MODE));
    this.consistencyMode = ExternalConsistencyMode.valueOf(jobConf.get(KuduWriterOptions.SESSION_EXTERNAL_CONSISTENCY_MODE));

    this.mutationBufferSize = jobConf.get(KuduWriterOptions.MUTATION_BUFFER_SIZE);
    this.flushInterval = jobConf.get(KuduWriterOptions.SESSION_FLUSH_INTERVAL);
    this.timeout = jobConf.get(KuduWriterOptions.SESSION_TIMEOUT_MS);
    this.ignoreDuplicateRows = jobConf.get(KuduWriterOptions.IGNORE_ALL_DUPLICATE_ROWS);

    validate();
  }

  /**
   * Check if configurations are valid.
   */
  public void validate() {
    Preconditions.checkState(mutationBufferSize == null || mutationBufferSize > 0);
    Preconditions.checkState(flushInterval == null || flushInterval > 0);
    Preconditions.checkState(timeout == null || timeout >= 0);
  }
}
