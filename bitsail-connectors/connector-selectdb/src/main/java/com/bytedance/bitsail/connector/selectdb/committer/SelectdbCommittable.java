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

package com.bytedance.bitsail.connector.selectdb.committer;

import lombok.Data;

import java.io.Serializable;

@Data
public class SelectdbCommittable implements Serializable {
  private final String hostPort;
  private final String clusterName;
  private final String copySQL;

  public SelectdbCommittable(String hostPort, String clusterName, String copySQL) {
    this.hostPort = hostPort;
    this.clusterName = clusterName;
    this.copySQL = copySQL;
  }
}
