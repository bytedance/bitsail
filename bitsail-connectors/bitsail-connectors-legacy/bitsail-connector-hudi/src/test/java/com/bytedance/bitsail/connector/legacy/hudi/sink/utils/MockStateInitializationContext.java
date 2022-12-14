/*
 * Copyright 2022-present, Bytedance Ltd.
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

import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.KeyGroupStatePartitionStreamProvider;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StatePartitionStreamProvider;

/**
 * A {@link FunctionInitializationContext} for testing purpose.
 */
public class MockStateInitializationContext implements StateInitializationContext {

  private final MockOperatorStateStore operatorStateStore;

  public MockStateInitializationContext() {
    operatorStateStore = new MockOperatorStateStore();
  }

  @Override
  public boolean isRestored() {
    return false;
  }

  @Override
  public MockOperatorStateStore getOperatorStateStore() {
    return operatorStateStore;
  }

  @Override
  public KeyedStateStore getKeyedStateStore() {
    return operatorStateStore;
  }

  @Override
  public Iterable<StatePartitionStreamProvider> getRawOperatorStateInputs() {
    return null;
  }

  @Override
  public Iterable<KeyGroupStatePartitionStreamProvider> getRawKeyedStateInputs() {
    return null;
  }
}
