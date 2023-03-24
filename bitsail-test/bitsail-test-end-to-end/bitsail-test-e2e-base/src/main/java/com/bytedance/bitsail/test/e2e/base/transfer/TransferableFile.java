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

package com.bytedance.bitsail.test.e2e.base.transfer;

import com.bytedance.bitsail.common.util.Preconditions;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;

import java.nio.file.Path;
import java.nio.file.Paths;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TransferableFile {
  /**
   * Absolute path in host.
   */
  @JsonProperty("host-path")
  private String hostPath;

  /**
   * Absolute path in container.
   */
  @JsonProperty("executor-path")
  private String containerPath;

  public void checkExist() {
    Path filePath = Paths.get(hostPath);
    Preconditions.checkState(filePath.toFile().exists(),
        "File " + hostPath + " doesn't exist");
  }

  @Override
  public String toString() {
    return String.format("{\"host_path\":\"%s\", \"container_path\":\"%s\"}", hostPath, containerPath);
  }

  @Override
  public int hashCode() {
    return this.toString().hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof TransferableFile
        && StringUtils.equals(hostPath, ((TransferableFile) obj).hostPath)
        && StringUtils.equals(containerPath, ((TransferableFile) obj).containerPath);
  }
}

