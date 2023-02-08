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

package com.bytedance.bitsail.connector.legacy.hudi.sink.utils;

import org.apache.flink.api.common.state.ValueState;

/**
 * Mock value state for testing.
 *
 * @param <V> Type of state value
 */
public class MockValueState<V> implements ValueState<V> {
  @SuppressWarnings("checkstyle:MemberName")
  private V v = null;

  @Override
  public V value() {
    return v;
  }

  @Override
  public void update(V value) {
    this.v = value;
  }

  @Override
  public void clear() {
    v = null;
  }
}
