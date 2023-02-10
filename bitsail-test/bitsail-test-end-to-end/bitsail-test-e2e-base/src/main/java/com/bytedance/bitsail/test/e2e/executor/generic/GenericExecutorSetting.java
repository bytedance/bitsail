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

package com.bytedance.bitsail.test.e2e.executor.generic;

import com.bytedance.bitsail.test.e2e.base.transfer.TransferableFile;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Getter;

import java.util.List;

@Getter
@Builder
public class GenericExecutorSetting {
  @JsonProperty("name")
  private final String executorName;

  @JsonProperty("docker-image")
  private final String dockerImage;

  @JsonProperty("engine-libs")
  private final List<TransferableFile> engineLibs;

  @JsonProperty("exec-commands")
  private final List<String> execCommands;

  @JsonProperty("failure-handle-commands")
  private final List<String> failureHandleCommands;

  @JsonProperty("global-job-config")
  private final String globalJobConf;
}
