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

package com.bytedance.bitsail.connector.hadoop.source.split;

import com.bytedance.bitsail.base.connector.reader.v1.SourceSplit;

public class HadoopSourceSplit implements SourceSplit {

  private static final long serialVersionUID = 1L;

  private final String splitId;

  public HadoopSourceSplit(String splitId) {
    this.splitId = splitId;
  }

  @Override
  public String uniqSplitId() {
    return splitId;
  }
}