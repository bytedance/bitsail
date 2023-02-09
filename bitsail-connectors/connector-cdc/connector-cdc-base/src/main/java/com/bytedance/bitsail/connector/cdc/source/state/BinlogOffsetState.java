/*
 * Copyright 2022-2023 Bytedance Ltd. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.bytedance.bitsail.connector.cdc.source.state;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Store mysql offset in Flink State.
 */
public class BinlogOffsetState implements Serializable {
  private static final long serialVersionUID = 1L;

  Map<String, String> offsetStore;

  public BinlogOffsetState() {
    this.offsetStore = new HashMap<>();
  }

  public BinlogOffsetState(Map<String, String> offsetStore) {
    this.offsetStore = offsetStore;
  }
}
