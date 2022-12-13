/*
 * Copyright [2022] [ByteDance]
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

package com.bytedance.bitsail.connector.legacy.hudi.compact;

import org.apache.hudi.common.model.CompactionOperation;

import java.io.Serializable;

/**
 * Represents a compact command from the compaction plan task {@link CompactionPlanOperator}.
 */
public class CompactionPlanEvent implements Serializable {
  private static final long serialVersionUID = 1L;

  private String compactionInstantTime;

  private CompactionOperation operation;

  public CompactionPlanEvent() {
  }

  public CompactionPlanEvent(String instantTime, CompactionOperation operation) {
    this.compactionInstantTime = instantTime;
    this.operation = operation;
  }

  public String getCompactionInstantTime() {
    return compactionInstantTime;
  }

  public void setCompactionInstantTime(String compactionInstantTime) {
    this.compactionInstantTime = compactionInstantTime;
  }

  public CompactionOperation getOperation() {
    return operation;
  }

  public void setOperation(CompactionOperation operation) {
    this.operation = operation;
  }
}
