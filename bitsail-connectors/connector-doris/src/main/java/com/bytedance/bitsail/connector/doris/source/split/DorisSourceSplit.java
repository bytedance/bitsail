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

package com.bytedance.bitsail.connector.doris.source.split;

import com.bytedance.bitsail.base.connector.reader.v1.SourceSplit;
import com.bytedance.bitsail.connector.doris.rest.model.PartitionDefinition;

import lombok.Getter;
import lombok.Setter;

/**
 * A {@link SourceSplit} that represents a {@link PartitionDefinition}.
 **/
@Getter
@Setter
public class DorisSourceSplit implements SourceSplit {

  private String splitId;
  private PartitionDefinition partitionDefinition;

  public DorisSourceSplit(PartitionDefinition partitionDefinition) {
    this.partitionDefinition = partitionDefinition;
    this.splitId = partitionDefinition.getBeAddress();
  }

  @Override
  public String uniqSplitId() {
    return splitId;
  }

}
